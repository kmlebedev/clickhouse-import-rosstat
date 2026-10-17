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
		{newsDimCBChannel, "credit_transmission"},
		{newsDimCBChannel, "macroprudential"},
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
}
