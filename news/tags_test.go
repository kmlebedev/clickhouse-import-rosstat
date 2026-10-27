package news

import "testing"

// TestNewsTagsRegistry — контракт реестра тегов: слаги уникальны в пределах измерения,
// у каждого тега есть расшифровка, у каналов ставки — статический знак влияния на инфляцию.
// Реестр — единственный источник правды для промпта и валидации, поэтому дубль или пустое
// поле здесь означали бы тег, который модель никогда не увидит или который нечем объяснить.
func TestNewsTagsRegistry(t *testing.T) {
	seen := make(map[string]bool, len(newsTags))
	counts := make(map[string]int, 5)
	for i, tag := range newsTags {
		if tag.Dimension == "" || tag.Slug == "" {
			t.Errorf("тег %d: пустые dimension/slug: %+v", i, tag)
			continue
		}
		key := tag.Dimension + ":" + tag.Slug
		if seen[key] {
			t.Errorf("тег %q повторяется: пара (dimension, slug) обязана быть уникальной", key)
		}
		seen[key] = true
		counts[tag.Dimension]++
		if tag.Title == "" {
			t.Errorf("тег %q: пустой Title — агенту нечем расшифровать слаг", key)
		}
		if tag.Dimension == newsDimCBChannel {
			if tag.Direction == "" {
				t.Errorf("тег %q: пустой Direction — у канала ставки знак обязателен (KEY_RATE_CHANNELS.md §3)", key)
			} else if tag.Direction != newsDirectionPro && tag.Direction != newsDirectionDez {
				t.Errorf("тег %q: Direction = %q, want %s|%s", key, tag.Direction, newsDirectionPro, newsDirectionDez)
			}
			continue
		}
		// У person/country/sector/event/theme знак задаёт конкретная новость, а не реестр.
		if tag.Direction != "" {
			t.Errorf("тег %q: Direction = %q, want пусто (статический знак только у cb_channel)", key, tag.Direction)
		}
	}

	if len(newsTags) != 46 {
		t.Errorf("newsTags = %d тегов, want 46 (8 персон + 7 стран + 8 секторов + 12 событий + 11 каналов)", len(newsTags))
	}
	for dim, want := range map[string]int{
		newsDimPerson:    8,
		newsDimCountry:   7,
		newsDimSector:    8,
		newsDimEvent:     12,
		newsDimCBChannel: 11,
	} {
		if counts[dim] != want {
			t.Errorf("измерение %s: тегов %d, want %d", dim, counts[dim], want)
		}
	}
	if counts[newsDimTheme] != 0 {
		t.Errorf("измерение theme: тегов %d, want 0 (стартовый набор не дублирует event)", counts[newsDimTheme])
	}
}

// TestNewsTagByKeyPinsKeySlugs — ключевые слаги спеки §4.4 находятся поиском по реестру,
// а выдуманный слаг не находится. Пин держит и наличие тега, и точное написание слага:
// опечатка в реестре иначе всплыла бы только после живого прогона LLM.
func TestNewsTagByKeyPinsKeySlugs(t *testing.T) {
	for _, want := range []struct {
		dimension, slug string
	}{
		{newsDimPerson, "trump"},
		{newsDimPerson, "nabiullina"},
		{newsDimCountry, "china"},
		{newsDimSector, "oil_gas"},
		{newsDimEvent, "energy_transition"},
		{newsDimEvent, "deglobalization"},
		{newsDimCBChannel, "budget_impulse"},
		{newsDimCBChannel, "lending_transmission"},
		{newsDimCBChannel, "autonomous_factors"},
	} {
		tag, ok := newsTagByKey(want.dimension, want.slug)
		if !ok {
			t.Errorf("newsTagByKey(%q, %q) = false, want тег из реестра", want.dimension, want.slug)
			continue
		}
		if tag.Dimension != want.dimension || tag.Slug != want.slug {
			t.Errorf("newsTagByKey(%q, %q) = %+v, want поля из ключа", want.dimension, want.slug, tag)
		}
	}
	if tag, ok := newsTagByKey(newsDimPerson, "nobody"); ok {
		t.Errorf("newsTagByKey(person, nobody) = %+v, want false: тега вне реестра быть не может", tag)
	}
	// Слаг существует, но в другом измерении — поиск по паре обязан его не найти.
	if _, ok := newsTagByKey(newsDimSector, "trump"); ok {
		t.Error("newsTagByKey(sector, trump) = true, want false: пара (dimension, slug) обязана совпадать целиком")
	}
	// Legacy-слаги живут только в истории news_annotation: реестр и промпт видят канон.
	for _, legacy := range []string{"credit_transmission", "external_conditions", "savings", "macroprudential"} {
		if tag, ok := newsTagByKey(newsDimCBChannel, legacy); ok {
			t.Errorf("newsTagByKey(cb_channel, %q) = %+v, want false: legacy-слаг вне реестра", legacy, tag)
		}
	}
}

// TestNewsTagAliases — legacy-слаги каналов остаются только в исторических строках
// news_annotation (prompt_version ≤ 2): каждый алиас ведёт на канонический cb_channel-тег,
// сам в реестре не встречается ни в одном измерении (иначе промпт и валидация приняли бы
// legacy за новый тег), а витрины обязаны нормализовать его на чтении.
func TestNewsTagAliases(t *testing.T) {
	if len(newsTagAliases) != 4 {
		t.Fatalf("алиасов %d, want 4 (credit_transmission, external_conditions, savings, macroprudential)", len(newsTagAliases))
	}
	seen := make(map[string]bool, len(newsTagAliases))
	for _, alias := range newsTagAliases {
		if alias.Dimension != newsDimCBChannel {
			t.Errorf("алиас %s:%s: dimension = %q, want %s", alias.Dimension, alias.Legacy, alias.Dimension, newsDimCBChannel)
		}
		if seen[alias.Legacy] {
			t.Errorf("алиас %q повторяется", alias.Legacy)
		}
		seen[alias.Legacy] = true
		if _, ok := newsTagByKey(alias.Dimension, alias.Canonical); !ok {
			t.Errorf("алиас %s:%s: канон %q отсутствует в реестре", alias.Dimension, alias.Legacy, alias.Canonical)
		}
		for _, tag := range newsTags {
			if tag.Slug == alias.Legacy {
				t.Errorf("алиас %q встречается в реестре (%s) — legacy обязан остаться вне newsTags", alias.Legacy, tag.Dimension)
			}
			if tag.Slug == alias.Canonical && tag.Dimension != alias.Dimension {
				t.Errorf("канон %q есть в чужом измерении %s", alias.Canonical, tag.Dimension)
			}
		}
	}
	// Цепочки алиасов запрещены: legacy не может вести на другой legacy.
	for _, alias := range newsTagAliases {
		if seen[alias.Canonical] {
			t.Errorf("алиас %q ведёт на legacy %q: цепочка алиасов запрещена", alias.Legacy, alias.Canonical)
		}
	}
}

// TestNewsCBChannelSlugsCanonical — 11 тегов cb_channel повторяют канон
// cbr/mp_summary_channels.go (KEY_RATE_CHANNELS.md §3.1) и его порядок. Пин держит и набор,
// и написание: расхождение с реестром разметки резюме — это снова три разных реестра.
func TestNewsCBChannelSlugsCanonical(t *testing.T) {
	want := []string{
		"current_inflation", "core_inflation", "expectations", "labor_market",
		"lending_transmission", "output_gap", "budget_impulse", "money_supply",
		"external_fx", "savings_behavior", "autonomous_factors",
	}
	var got []string
	for _, tag := range newsTags {
		if tag.Dimension == newsDimCBChannel {
			got = append(got, tag.Slug)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("cb_channel-тегов %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cb_channel[%d] = %q, want %q (порядок cbr/mp_summary_channels.go)", i, got[i], want[i])
		}
	}
}
