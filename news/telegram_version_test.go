package news

import (
	"fmt"
	"testing"
	"time"
)

// telegramTestItem — текущее сообщение Telegram-окна для офлайн-тестов diffTelegram.
func telegramTestItem(id uint64, date time.Time, title, text string) newsItem {
	return newsItem{
		Date:        date,
		Source:      "interfax",
		Title:       title,
		URL:         fmt.Sprintf("https://t.me/interfaxonline/%d", id),
		Description: text,
		ContentHash: contentHash(title, text),
		Channel:     "interfaxonline",
		MessageID:   id,
	}
}

// telegramTestVersion — сохранённая живая версия для офлайн-тестов diffTelegram.
func telegramTestVersion(id uint64, version uint32, date time.Time, title, text string) telegramVersion {
	return telegramVersion{
		Channel:     "interfaxonline",
		MessageID:   id,
		Version:     version,
		Date:        date,
		Source:      "interfax",
		Title:       title,
		URL:         fmt.Sprintf("https://t.me/interfaxonline/%d", id),
		Description: text,
		ContentHash: contentHash(title, text),
		FirstSeen:   date.Add(-time.Hour),
	}
}

// findVersion ищет версию по message_id/version: порядок строк батча не должен быть
// предметом проверки там, где важен состав.
func findVersion(t *testing.T, versions []telegramVersion, id uint64, version uint32) telegramVersion {
	t.Helper()
	for _, v := range versions {
		if v.MessageID == id && v.Version == version {
			return v
		}
	}
	t.Fatalf("версия message_id=%d version=%d не найдена в %+v", id, version, versions)
	return telegramVersion{}
}

func TestDiffTelegramFirstVersions(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	d1 := now.Add(-2 * time.Hour)
	d2 := now.Add(-time.Hour)
	current := []newsItem{
		telegramTestItem(1, d1, "Первая молния", "Текст первой молнии"),
		telegramTestItem(2, d2, "Вторая молния", "Текст второй молнии"),
	}
	seen := map[uint64]struct{}{1: {}, 2: {}}

	items, versions, stats := diffTelegram(nil, current, seen, d1, d2, now)

	if len(items) != len(current) {
		t.Fatalf("items = %d, want %d (все текущие сообщения идут в upsert news_events)", len(items), len(current))
	}
	for i, it := range items {
		if it.Title != current[i].Title || it.Description != current[i].Description || it.ContentHash != current[i].ContentHash {
			t.Errorf("items[%d] = %+v, want текущие поля %+v", i, it, current[i])
		}
		if it.Channel != "interfaxonline" || it.MessageID != current[i].MessageID {
			t.Errorf("items[%d]: Channel/MessageID = %q/%d, want interfaxonline/%d",
				i, it.Channel, it.MessageID, current[i].MessageID)
		}
	}
	if len(versions) != 2 {
		t.Fatalf("versions = %d, want 2 (по v1 на сообщение): %+v", len(versions), versions)
	}
	for _, id := range []uint64{1, 2} {
		v := findVersion(t, versions, id, 1)
		if v.Deleted != 0 {
			t.Errorf("v1 сообщения %d: Deleted = %d, want 0", id, v.Deleted)
		}
		if !v.FirstSeen.Equal(now) {
			t.Errorf("v1 сообщения %d: FirstSeen = %v, want время прогона %v", id, v.FirstSeen, now)
		}
		if v.Title == "" || v.URL == "" || v.Source != "interfax" {
			t.Errorf("v1 сообщения %d потеряла поля: %+v", id, v)
		}
	}
	if want := (telegramDiffStats{NewVersions: 2, Deleted: 0, Unchanged: 0}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramUnchangedNoNewVersion(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	date := now.Add(-time.Hour)
	current := []newsItem{telegramTestItem(1, date, "Заголовок", "Текст")}
	existing := []telegramVersion{telegramTestVersion(1, 1, date, "Заголовок", "Текст")}

	items, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, date, date, now)

	if len(items) != 1 {
		t.Fatalf("items = %d, want 1 (текущее сообщение пишется всегда)", len(items))
	}
	if len(versions) != 0 {
		t.Errorf("versions = %+v, want пусто: hash не изменился — новой версии нет", versions)
	}
	if want := (telegramDiffStats{Unchanged: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramEditAddsVersionKeepsV1Title(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	date := now.Add(-time.Hour)
	existing := []telegramVersion{telegramTestVersion(1, 1, date, "Старая первая строка", "Старый текст")}
	current := []newsItem{telegramTestItem(1, date, "Новая первая строка", "Новый текст")}

	items, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, date, date, now)

	if len(versions) != 1 {
		t.Fatalf("versions = %d, want 1 (правка — одна новая версия): %+v", len(versions), versions)
	}
	v := versions[0]
	if v.Version != 2 || v.Deleted != 0 {
		t.Errorf("версия правки = v%d deleted=%d, want v2 deleted=0", v.Version, v.Deleted)
	}
	if v.Title != current[0].Title || v.Description != current[0].Description || v.ContentHash != current[0].ContentHash {
		t.Errorf("версия правки = %+v, want текущий текст %+v", v, current[0])
	}
	if !v.Date.Equal(date) {
		t.Errorf("date версии = %v, want дату публикации %v (не время прогона)", v.Date, date)
	}
	if !v.FirstSeen.Equal(now) {
		t.Errorf("first_seen версии = %v, want время прогона %v", v.FirstSeen, now)
	}
	// Ключ news_events (date, source, title) не должен разъезжаться: title фиксируется по v1.
	if items[0].Title != "Старая первая строка" {
		t.Errorf("items[0].Title = %q, want title версии 1 (стабильный ключ news_events)", items[0].Title)
	}
	if items[0].Description != "Новый текст" || items[0].ContentHash != current[0].ContentHash {
		t.Errorf("items[0] = %+v, want текущие description/content_hash", items[0])
	}
	if want := (telegramDiffStats{NewVersions: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramTombstoneInWindow(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	windowMax := now.Add(-time.Hour)
	current := []newsItem{telegramTestItem(1, windowMin.Add(30*time.Minute), "Живая", "Текст живой")}
	// Удалённое сообщение сохранилось как v1 и дата публикации внутри окна.
	existing := []telegramVersion{telegramTestVersion(2, 1, windowMin.Add(15*time.Minute), "Удалённая", "Текст удалённой")}

	items, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, windowMin, windowMax, now)

	if len(items) != 1 {
		t.Fatalf("items = %d, want 1 (tombstone в news_events не пишется)", len(items))
	}
	if len(versions) != 2 {
		t.Fatalf("versions = %d, want 2 (v1 живой + tombstone): %+v", len(versions), versions)
	}
	tomb := findVersion(t, versions, 2, 2)
	if tomb.Deleted != 1 {
		t.Errorf("tombstone: Deleted = %d, want 1", tomb.Deleted)
	}
	if tomb.Description != "" || tomb.ContentHash != "" {
		t.Errorf("tombstone: description/content_hash = %q/%q, want пустые", tomb.Description, tomb.ContentHash)
	}
	if tomb.Title != "Удалённая" || tomb.Source != "interfax" || tomb.URL == "" {
		t.Errorf("tombstone = %+v, want title/source/url из последней живой версии", tomb)
	}
	if !tomb.FirstSeen.Equal(now) {
		t.Errorf("tombstone: FirstSeen = %v, want время прогона %v", tomb.FirstSeen, now)
	}
	if want := (telegramDiffStats{NewVersions: 1, Deleted: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramOutsideWindowUntouched(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	windowMax := now.Add(-time.Hour)

	for _, tc := range []struct {
		name string
		date time.Time
	}{
		{"раньше окна", windowMin.Add(-time.Minute)},
		{"позже окна", windowMax.Add(time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := []newsItem{telegramTestItem(1, windowMin, "Текущая", "Текст")}
			existing := []telegramVersion{telegramTestVersion(2, 1, tc.date, "Вышла из окна", "Текст")}

			_, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, windowMin, windowMax, now)

			if len(versions) != 1 {
				t.Errorf("versions = %+v, want только v1 текущего сообщения: сообщение за окном не трогается", versions)
			}
			if stats.Deleted != 0 {
				t.Errorf("stats.Deleted = %d, want 0: выход из окна — не удаление", stats.Deleted)
			}
		})
	}
}

func TestDiffTelegramSeenIDsProtectMedia(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	windowMax := now.Add(-time.Hour)
	current := []newsItem{telegramTestItem(1, windowMin, "Текстовая", "Текст")}
	existing := []telegramVersion{telegramTestVersion(2, 1, windowMin.Add(10*time.Minute), "Медиа без текста", "")}

	// ID медиа-сообщения есть в SeenIDs, хотя позиции (текста) у него нет.
	_, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}, 2: {}}, windowMin, windowMax, now)

	if len(versions) != 1 {
		t.Errorf("versions = %+v, want только v1 текущего: медиа в SeenIDs не удаляется", versions)
	}
	if stats.Deleted != 0 {
		t.Errorf("stats.Deleted = %d, want 0 (SeenIDs защищает медиа)", stats.Deleted)
	}
}

func TestDiffTelegramNilSeenNoTombstone(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	windowMax := now.Add(-time.Hour)
	current := []newsItem{telegramTestItem(1, windowMin, "Текущая", "Текст")}
	existing := []telegramVersion{telegramTestVersion(2, 1, windowMin.Add(time.Minute), "Из сбойной ленты", "Текст")}

	// seen == nil — лента не прочиталась: отсутствие ID ничего не доказывает.
	_, versions, stats := diffTelegram(existing, current, nil, windowMin, windowMax, now)

	if len(versions) != 1 {
		t.Errorf("versions = %+v, want только v1 текущего: при seen == nil tombstone не делается", versions)
	}
	if stats.Deleted != 0 {
		t.Errorf("stats.Deleted = %d, want 0 (сбой ленты не должен удалить канал)", stats.Deleted)
	}
}

func TestDiffTelegramDeletedMessageReturnAddsLiveVersion(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	date := now.Add(-time.Hour)
	existing := []telegramVersion{
		telegramTestVersion(1, 1, date, "Первая строка v1", "Текст v1"),
		{ // tombstone: сообщение исчезало из окна
			Channel: "interfaxonline", MessageID: 1, Version: 2, Date: date, Source: "interfax",
			Title: "Первая строка v1", URL: "https://t.me/interfaxonline/1",
			FirstSeen: date.Add(30 * time.Minute), Deleted: 1,
		},
	}
	current := []newsItem{telegramTestItem(1, date, "Первая строка v3", "Вернувшийся текст")}

	items, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, date, date, now)

	if len(versions) != 1 {
		t.Fatalf("versions = %d, want 1 (возврат — новая живая версия): %+v", len(versions), versions)
	}
	v := versions[0]
	if v.Version != 3 || v.Deleted != 0 {
		t.Errorf("версия возврата = v%d deleted=%d, want v3 deleted=0", v.Version, v.Deleted)
	}
	if v.ContentHash != current[0].ContentHash {
		t.Errorf("версия возврата: ContentHash = %q, want текущий %q", v.ContentHash, current[0].ContentHash)
	}
	if items[0].Title != "Первая строка v1" {
		t.Errorf("items[0].Title = %q, want title v1 (ключ news_events стабилен)", items[0].Title)
	}
	if want := (telegramDiffStats{NewVersions: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramLatestVersionWins(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	date := now.Add(-time.Hour)
	existing := []telegramVersion{
		telegramTestVersion(1, 1, date, "v1", "Текст"),
		telegramTestVersion(1, 2, date, "v2", "Текст v2"),
		telegramTestVersion(1, 3, date, "v3", "Текст v3"),
	}
	current := []newsItem{telegramTestItem(1, date, "v3", "Текст v3")}

	items, versions, stats := diffTelegram(existing, current, map[uint64]struct{}{1: {}}, date, date, now)

	if len(versions) != 0 {
		t.Errorf("versions = %+v, want пусто: hash совпал с последней версией v3", versions)
	}
	if stats.Unchanged != 1 {
		t.Errorf("stats.Unchanged = %d, want 1", stats.Unchanged)
	}
	if items[0].Title != "v1" {
		t.Errorf("items[0].Title = %q, want title версии 1", items[0].Title)
	}
}

func TestDiffTelegramRepeatedRunIsNoop(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	current := []newsItem{
		telegramTestItem(1, windowMin, "Первая", "Текст первой"),
		telegramTestItem(2, windowMin.Add(30*time.Minute), "Вторая", "Текст второй"),
	}
	seen := map[uint64]struct{}{1: {}, 2: {}}

	items, versions, stats := diffTelegram(nil, current, seen, windowMin, windowMin.Add(time.Hour), now)
	if len(versions) != 2 || stats.NewVersions != 2 {
		t.Fatalf("первый прогон: versions=%d stats=%+v, want 2 новых версии", len(versions), stats)
	}

	// Второй прогон с уже записанными версиями — no-op: ни новых версий, ни tombstone.
	_, again, stats := diffTelegram(versions, items, seen, windowMin, windowMin.Add(time.Hour), now.Add(30*time.Minute))
	if len(again) != 0 {
		t.Errorf("повторный прогон: versions = %+v, want пусто (запись идемпотентна)", again)
	}
	if want := (telegramDiffStats{Unchanged: 2}); stats != want {
		t.Errorf("повторный прогон: stats = %+v, want %+v", stats, want)
	}
}

func TestDiffTelegramRepeatedTombstoneIsNoop(t *testing.T) {
	now := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	windowMin := now.Add(-2 * time.Hour)
	current := []newsItem{telegramTestItem(1, windowMin, "Живая", "Текст")}
	seen := map[uint64]struct{}{1: {}}
	// Удалённое сообщение уже имеет tombstone v2.
	existing := []telegramVersion{
		telegramTestVersion(2, 1, windowMin.Add(time.Minute), "Удалённая", "Текст"),
		{Channel: "interfaxonline", MessageID: 2, Version: 2, Date: windowMin.Add(time.Minute),
			Source: "interfax", Title: "Удалённая", URL: "https://t.me/interfaxonline/2",
			FirstSeen: now.Add(-time.Hour), Deleted: 1},
	}

	_, versions, stats := diffTelegram(existing, current, seen, windowMin, windowMin.Add(time.Hour), now)
	if len(versions) != 1 {
		t.Errorf("versions = %+v, want только v1 текущего: повторный tombstone не пишется", versions)
	}
	if stats.Deleted != 0 {
		t.Errorf("stats.Deleted = %d, want 0: tombstone уже записан", stats.Deleted)
	}
}
