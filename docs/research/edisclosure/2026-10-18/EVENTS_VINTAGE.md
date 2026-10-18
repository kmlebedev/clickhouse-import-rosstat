# Event-vintage: как получить версии событий

Документный архив (`registry.jsonl`, `metadata.jsonl`, `files/`) отвечает на вопрос «какая версия файла была опубликована и когда». Для событий нужен отдельный слой: публичная страница e-disclosure показывает **текущее** сообщение, а не историю `Publish/Change/Delete/Exclude/Restore`.

## Источники версий

### API «Шлюз данных»

`GET /v1/disclosure/events`:

- `entity=Messages|Files`;
- `fromEventId` / `toEventId` — режим синхронизации (инкремент);
- `fromEventDate` / `toEventDate` — окно (минимум 01.07.2020);
- `subjectCode` — ИНН/ОГРН/код эмитента (до 5);
- `type` — тип сообщения/документа (до 5).

Ответ по сообщению: `uid`, `date`, `type` (`Publish`, `Change`, `Delete`, `Exclude`, `Restore`), `subject` (ИНН, ОГРН, shortName, disclosureUrl), `message` (`uid`, `type`, `text`, `agency`, `publicUrl`, `attachments`, `header`).

Для архива версий сохранять: `event_uid`, `action`, `publication_ts`, `event_type`, `issuer`, `text` (или его `sha256`), `document_uid` (если есть), `publicUrl`.

### FTP

Ежедневный XML за предыдущие сутки:

- `EVENT_TYPE`, `MESSAGE_ID`, `REG_TYPE` (0 добавление, 1 обновление, 2 удаление, 4 уведомить биржи), `TIMESTAMP`, `EVENT_HEADER`, `EVENT_DATE`, `TEXT_RUS`;
- отдельный XML `SignificantEventInfo` с `OperationType="Exclusion"` для исключений из публичного доступа.

Имя файла — идентификатор события; каждая версия ложится отдельным файлом.

### Публичный сайт

Даёт текущее состояние и документы, но не историю версий. Регулярный опрос может фиксировать изменения **вперёд** с момента первого опроса: `first_seen` + хеш текста. Восстановить версии задним числом скрейпингом нельзя.

## Схема архивной записи

Для event-версий предлагается тот же `archive.Entry` плюс sidecar:

```
source: "edisclosure_events"
name: "<event_uid>_<action>_<publication_ts>.xml"
fingerprint: sha256 содержимого XML/JSON
sha256: sha256 содержимого
size: ...
url: publicUrl
key: "edisclosure_events/<sha256[:16]>_<name>"
first_seen: ...
```

Sidecar `metadata.jsonl`:

```
event_uid, action, event_type, event_date, publication_ts,
issuer, company_id, inn, document_uid, text_sha256, note
```

В ClickHouse это может быть `corporate_events` с ключом `(event_uid, action, publication_ts)`; `ReplacingMergeTree` не схлопнет разные действия, потому что действие и время публикации входят в ключ.

## Что делать

1. **Вперёд:** даже без платного канала можно опрашивать публичную страницу/поиск и фиксировать `first_seen` + хеш; это даст версии с сегодняшнего дня, но не историю.
2. **История:** API-архив с 01.07.2020 (+50% к тарифу) или FTP с даты подписки.
3. **Приёмка:** для каждого события проверить, что `Change/Delete/Exclude` дают отдельную строку, а не затирают исходную; повторный прогон — no-op.
