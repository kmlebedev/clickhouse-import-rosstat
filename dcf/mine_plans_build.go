package dcf

// buildPlanYears разворачивает сид одного актива в строки mine_plans по годам.
//
// Функция ЧИСТАЯ: ни БД, ни логгера. Причина в том, что единственный внешний
// вход — факт добычи (последний отчётный год), а всё остальное — публичные
// константы; держать это вне I/O позволяет проверять план офлайн-тестом и не
// тащить в модель соединение с ClickHouse.
//
// baseProductionKoz — последний ФАКТ добычи из databook_polyus, тыс. унц. Он
// игнорируется, если у актива задан ProductionProfile: у проекта развития
// (Сухой Лог) строки факта нет, и профиль — единственный источник баз.
//
// ВАЖНО: ноль в baseProductionKoz — это отсутствие факта, а НЕ нулевая добыча.
// Функция возвращает nil, а вызывающий пишет warn и не пишет строк вовсе: год с
// нулевой добычей на месте пропущенного факта тихо занизил бы NPV, тогда как
// отсутствие строк видно в логе и в счётчике импорта.
//
// ClosureCosts сида, равный нулю, — тоже отсутствие входа (см. комментарий к
// assetPlanSeed.ClosureCosts): распределение группового провижена не утверждено,
// и подставлять измеренный ноль запрещено. Поэтому хвост кладётся в последний
// год ПУСТЫМ (0), но СТРУКТУРНО — именно в последний год, как читает npvLOM
// (dcf/model.go): когда распределение появится, оно встанет в готовое место.
func buildPlanYears(seed assetPlanSeed, baseProductionKoz float64) []MinePlanYear {
	// Профиль (Сухой Лог) — отдельная ветвь: он сам задаёт и первую год-строку,
	// и добычу каждого года, поэтому факт здесь не нужен и не проверяется.
	if len(seed.ProductionProfile) > 0 {
		return buildProfileYears(seed)
	}

	if baseProductionKoz <= 0 {
		return nil
	}

	// AISC выводится из TCC актива плюс групповой клин «поддержание»: Полюс
	// публикует AISC только по Группе, и per-asset раскрытия не существует.
	// Отсюда derived-природа: клин один на все годы и активы.
	aisc := seed.TCC + sustainingWedgeUSDPerOz

	years := make([]MinePlanYear, 0, seed.MineLifeYears)
	for i := 0; i < seed.MineLifeYears; i++ {
		year := seed.BaseYear + uint16(i)

		production := baseProductionKoz
		// Консервация карьера: добыча ноль, но СТРОКА ОСТАЁТСЯ. Пропуск года
		// сдвинул бы yearIndex в npvLOM и дисконтировал бы хвост плана раньше
		// реального календаря — NPV считался бы на сжатом времени.
		if seed.HaltFrom != 0 && seed.HaltTo != 0 && year >= seed.HaltFrom && year <= seed.HaltTo {
			production = 0
		}

		years = append(years, MinePlanYear{
			Year:            year,
			ProductionKoz:   production,
			TCC:             seed.TCC,
			AISC:            aisc,
			CapexSustaining: seed.CapexSustaining,
			// CapexProject у действующих рудников нулевой: публикация не делит
			// capex на sustaining/project, и вся раскрытая сумма уже в sustaining.
			CapexProject: 0,
		})
	}

	applyClosureTail(years, seed.ClosureCosts)

	return years
}

// buildProfileYears строит план актива развития по ProductionProfile.
//
// Профиль, а не MineLifeYears, — авторитет по длине горизонта: у Сухого Лога
// публикуется ровно 10 лет первых линий ЗИФ, и молчаливое расхождение с полем
// срока службы не должно ни паниковать, ни обрезать профиль — иначе часть
// объявленной добычи исчезла бы из NAV без следа.
func buildProfileYears(seed assetPlanSeed) []MinePlanYear {
	n := len(seed.ProductionProfile)
	aisc := seed.TCC + sustainingWedgeUSDPerOz

	// ProjectCapex — ИТОГ по проекту, а не годовая ставка: $6 млрд — полный
	// объём инвестиций в Сухой Лог (презентация, стр. 22). Публикация не даёт
	// по-годичного графика, поэтому итог размазывается РАВНОМЕРНО по годам
	// профиля: сумма строк равна итогу, и это единственное свойство, которое
	// можно утверждать без графика. Раскладывать итог в КАЖДЫЙ год нельзя —
	// горизонт в 10 лет дал бы 60 млрд, то есть завышение инвестиций в 10 раз.
	//
	// Это приближение по ТАЙМИНГУ (какой год несёт какую долю), а не по сумме.
	// Настоящая годовая кривая capex из ТЭО заменит его; пробел помечен в
	// docs/DCF_DATA_COVERAGE.md (Task 7).
	perYear := seed.ProjectCapex / float64(n)

	years := make([]MinePlanYear, 0, n)
	for i, production := range seed.ProductionProfile {
		// Годы до ProfileStartYear не пишутся вовсе: актив ещё не производит, и
		// год-заглушка с нулём растянул бы горизонт дисконтирования впустую.
		year := seed.ProfileStartYear + uint16(i)

		// Последний год забирает ОСТАТОК итога: сумма n одинаковых долей в
		// float64 не равна исходному числу в общем случае, и без этого сумма
		// строк разошлась бы с опубликованными 6 000 на ошибку округления.
		projectCapex := perYear
		if i == n-1 {
			projectCapex = seed.ProjectCapex - perYear*float64(n-1)
		}

		years = append(years, MinePlanYear{
			Year:            year,
			ProductionKoz:   production,
			TCC:             seed.TCC,
			AISC:            aisc,
			CapexSustaining: seed.CapexSustaining,
			CapexProject:    projectCapex,
		})
	}

	applyClosureTail(years, seed.ClosureCosts)

	return years
}

// applyClosureTail кладёт хвост рекультивации в ПОСЛЕДНИЙ год плана.
//
// Место задано приёмкой модели: npvLOM добавляет ClosureCosts только на
// последней итерации (dcf/model.go), и он же входит в налоговую базу как
// вычитаемый расход — то есть должен быть отрицательным. Размазывание хвоста
// по годам или его отсутствие меняет и поток, и налог последнего года.
func applyClosureTail(years []MinePlanYear, closure float64) {
	if len(years) == 0 {
		return
	}

	years[len(years)-1].ClosureCosts = closure
}
