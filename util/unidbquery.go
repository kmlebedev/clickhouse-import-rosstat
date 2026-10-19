package util

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// UniDbQueryURL собирает GET-адрес формы ЦБ UniDbQuery: проставляет
// Posted/From/To, сохраняя уже существующие параметры base (например, x=1).
// Даты — в формате ЦБ 02.01.2006. Сеть не используется.
func UniDbQueryURL(base string, from, to time.Time) (string, error) {
	if strings.TrimSpace(base) == "" {
		return "", fmt.Errorf("UniDbQueryURL: пустой base URL")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("UniDbQueryURL: разбор base URL %q: %w", base, err)
	}

	q := u.Query()
	q.Set("UniDbQuery.Posted", "True")
	q.Set("UniDbQuery.From", from.Format("02.01.2006"))
	q.Set("UniDbQuery.To", to.Format("02.01.2006"))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// IsUniDbQueryPage отличает валидную страницу формы ЦБ с пустым результатом
// (таблицы нет, но скрытый input UniDbQuery.Posted присутствует) от WAF/error-заглушки.
func IsUniDbQueryPage(body []byte) bool {
	return bytes.Contains(body, []byte("UniDbQuery.Posted"))
}
