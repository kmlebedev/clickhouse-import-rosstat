package news

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// telegramVersionTable — винтажный слой Telegram-сообщений: одна строка на версию.
const telegramVersionTable = "news_message_version"

// telegramVersion — одна версия сообщения Telegram-канала. Version растёт на правку текста
// и на удаление (tombstone); Date — момент публикации и не меняется между версиями, FirstSeen —
// время прогона, впервые увидевшего версию. Deleted — 1 у tombstone, у которого Description и
// ContentHash пустые, а Title/URL/Date взяты из последней живой версии.
type telegramVersion struct {
	Channel     string
	MessageID   uint64
	Version     uint32
	Date        time.Time
	Source      string
	Title       string
	URL         string
	Description string
	ContentHash string
	FirstSeen   time.Time
	Deleted     uint8
}

// telegramDiffStats — итог сравнения окна канала с сохранёнными версиями для лога прогона.
// NewVersions — новые версии текущих сообщений (первые, правки и возвраты); Deleted — число
// tombstone-версий; Unchanged — текущие сообщения, чей hash не изменился и новой версии не дал.
// Сумма NewVersions+Deleted — число строк батча news_message_version.
type telegramDiffStats struct {
	NewVersions int
	Deleted     int
	Unchanged   int
}

const (
	// telegramVersionsQuery читает версии одного канала не старее since (нижней границы окна
	// этого прогона). Плейсхолдеры позиционные: ? #1 — channel, ? #2 — since (time.Time).
	// FINAL обязателен: news_message_version — ReplacingMergeTree, и без него повторно
	// записанная версия читалась бы дважды, искажая максимум version. Порядок строк —
	// часть контракта Scan: message_id, version.
	telegramVersionsQuery = `SELECT channel, message_id, version, date, source, title, url, description, content_hash, first_seen, deleted FROM ` + telegramVersionTable + ` FINAL WHERE channel = ? AND date >= ? ORDER BY message_id, version`

	// telegramVersionInsert — запись версии. Список колонок явный, и его порядок — контракт
	// с порядком аргументов Append в insertTelegramVersions (порядок канонического DDL).
	telegramVersionInsert = `INSERT INTO ` + telegramVersionTable + ` (channel, message_id, version, date, source, title, url, description, content_hash, first_seen, deleted)`
)

// readTelegramVersions читает сохранённые версии одного канала в окне прогона: все версии
// сообщений с датой публикации не старее since. Это вход diffTelegram — без истории правка
// выглядела бы первой версией, и нумерация затёрлась бы.
//
// Порядок Scan — порядок колонок telegramVersionsQuery: channel, message_id, version, date,
// source, title, url, description, content_hash, first_seen, deleted. Ошибка обхода проверяется:
// молча вернувшаяся половина истории дала бы повторные v1 и лишние tombstone.
func readTelegramVersions(ctx context.Context, conn driver.Conn, channel string, since time.Time) ([]telegramVersion, error) {
	rows, err := conn.Query(ctx, telegramVersionsQuery, channel, since)
	if err != nil {
		return nil, fmt.Errorf("news_watch: читать версии %s (channel=%s): %w", telegramVersionTable, channel, err)
	}
	defer func() { _ = rows.Close() }()

	var versions []telegramVersion
	for rows.Next() {
		var v telegramVersion
		if err = rows.Scan(&v.Channel, &v.MessageID, &v.Version, &v.Date, &v.Source, &v.Title,
			&v.URL, &v.Description, &v.ContentHash, &v.FirstSeen, &v.Deleted); err != nil {
			return nil, fmt.Errorf("news_watch: читать строку версии %s (channel=%s): %w", telegramVersionTable, channel, err)
		}
		versions = append(versions, v)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("news_watch: читать версии %s (channel=%s): %w", telegramVersionTable, channel, err)
	}
	return versions, nil
}

// insertTelegramVersions пишет версии одним батчем: построчный Exec в цикле запрещён
// (правило 2 AGENTS.md). Пустой список не открывает батч вовсе — повторный прогон без правок
// не должен слать пустой Send. Ошибка Append прерывает батч через Abort: частично собранный
// батч Send не отправляет, поэтому половина версий в таблицу не уедет.
//
// Порядок аргументов Append — контракт с явным списком колонок telegramVersionInsert
// (channel, message_id, version, date, source, title, url, description, content_hash,
// first_seen, deleted); менять его можно только вместе с ним.
func insertTelegramVersions(ctx context.Context, conn driver.Conn, versions []telegramVersion) (int64, error) {
	if len(versions) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, telegramVersionInsert)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, v := range versions {
		if err = batch.Append(v.Channel, v.MessageID, v.Version, v.Date, v.Source, v.Title,
			v.URL, v.Description, v.ContentHash, v.FirstSeen, v.Deleted); err != nil {
			_ = batch.Abort()
			return count, err
		}
		count++
	}
	err = batch.Send()
	return count, err
}

// diffTelegram сравнивает текущее окно Telegram-канала с сохранёнными версиями и решает, какие
// строки писать. Функция чистая (ни БД, ни сети) — весь контракт версий проверяется офлайн.
//
// Вход:
//   - existing — версии одного канала; для каждого message_id берётся максимум version.
//     Читаются с date >= windowMin, поэтому все версии сообщений окна на месте.
//   - current — текущие сообщения окна (только с текстом); SeenIDs ленты в current не входит.
//   - seen — все ID окна, включая медиа без текста; nil означает «окно не прочитано»
//     (сбой ленты), и тогда tombstone не делается вовсе: пустая карта не должна удалить
//     весь канал.
//   - windowMin/windowMax — границы окна по датам публикации; сообщение за окном просто
//     вышло из него и не трогается.
//   - now — время прогона для first_seen.
//
// Выход:
//   - items — все текущие сообщения для upsert news_events; Title фиксируется по версии 1
//     (если она есть), чтобы ключ (date, source, title) не разъезжался при правке первой
//     строки; Description/ContentHash остаются текущими.
//   - versions — новые версии: v1 для новых сообщений, v+1 при смене content_hash, возврат
//     удалённого сообщения (deleted=0) и tombstone (deleted=1) для известного сообщения
//     окна, которого нет в seen.
//   - stats — счётчики для лога прогона.
//
// Сообщение, чей hash не изменился и которое не удалено, новой версии не даёт; повторный
// вызов с уже записанными версиями — no-op.
func diffTelegram(existing []telegramVersion, current []newsItem, seen map[uint64]struct{},
	windowMin, windowMax, now time.Time) (items []newsItem, versions []telegramVersion, stats telegramDiffStats) {
	latest := make(map[uint64]telegramVersion, len(existing))
	firstTitle := make(map[uint64]string, len(existing))
	for _, v := range existing {
		if v.Version == 1 {
			firstTitle[v.MessageID] = v.Title
		}
		// При равной version (повторная запись той же строки) побеждает последняя:
		// содержимое у них одинаково, важен лишь детерминированный выбор.
		if cur, ok := latest[v.MessageID]; !ok || v.Version >= cur.Version {
			latest[v.MessageID] = v
		}
	}

	// currentIDs — защита от tombstone сообщения, которое в этом же прогоне пришло
	// текущим: seen обязан его содержать, но diff не должен зависеть от полноты карты.
	currentIDs := make(map[uint64]struct{}, len(current))
	items = make([]newsItem, 0, len(current))
	for _, it := range current {
		currentIDs[it.MessageID] = struct{}{}

		item := it
		if title, ok := firstTitle[it.MessageID]; ok {
			// Title в news_events — часть ключа (date, source, title), поэтому держим
			// заголовок версии 1: актуальная первая строка живёт в news_message_version.
			item.Title = title
		}
		items = append(items, item)

		last, ok := latest[it.MessageID]
		if !ok {
			versions = append(versions, telegramVersion{
				Channel:     it.Channel,
				MessageID:   it.MessageID,
				Version:     1,
				Date:        it.Date,
				Source:      it.Source,
				Title:       it.Title,
				URL:         it.URL,
				Description: it.Description,
				ContentHash: it.ContentHash,
				FirstSeen:   now,
				Deleted:     0,
			})
			stats.NewVersions++
			continue
		}
		if last.Deleted == 0 && last.ContentHash == it.ContentHash {
			stats.Unchanged++
			continue
		}
		// Правка (hash изменился) или возврат ранее удалённого сообщения: обе — новая
		// живая версия с текущим текстом. Date — момент публикации, он не меняется.
		versions = append(versions, telegramVersion{
			Channel:     it.Channel,
			MessageID:   it.MessageID,
			Version:     last.Version + 1,
			Date:        it.Date,
			Source:      it.Source,
			Title:       it.Title,
			URL:         it.URL,
			Description: it.Description,
			ContentHash: it.ContentHash,
			FirstSeen:   now,
			Deleted:     0,
		})
		stats.NewVersions++
	}

	// seen == nil — лента не прочиталась (окно неизвестно): отсутствие ID ничего
	// не доказывает, и tombstone не делаем. Пустую карту успешная лента не отдаёт
	// (ноль распознанных сообщений — ошибка parseTelegramPage и Err ленты), поэтому
	// сценарий «пустое окно удаляет весь канал» в Import не возникает.
	if seen == nil {
		return items, versions, stats
	}

	ids := make([]uint64, 0, len(latest))
	for id := range latest {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		last := latest[id]
		if last.Deleted != 0 {
			continue // уже tombstone: повторный прогон без возврата сообщения — no-op
		}
		if _, ok := currentIDs[id]; ok {
			continue // сообщение пришло текущим — версия уже решена выше
		}
		if _, ok := seen[id]; ok {
			continue // медиа без текста или битая позиция: сообщение в окне
		}
		if last.Date.Before(windowMin) || last.Date.After(windowMax) {
			continue // сообщение просто вышло из окна канала — это не удаление
		}
		versions = append(versions, telegramVersion{
			Channel:     last.Channel,
			MessageID:   id,
			Version:     last.Version + 1,
			Date:        last.Date,
			Source:      last.Source,
			Title:       last.Title,
			URL:         last.URL,
			Description: "",
			ContentHash: "",
			FirstSeen:   now,
			Deleted:     1,
		})
		stats.Deleted++
	}
	return items, versions, stats
}
