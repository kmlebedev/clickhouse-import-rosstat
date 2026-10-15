// Command grafana генерирует артефакты Grafana как код.
//
// Каждый дашборд контура (gold-thesis — §4 золотого роадмапа, key-rate-publications — публикации
// ключевой ставки) собирается в K8s-стиль манифест, который деплоится `gcx resources push`
// (make grafana-deploy-test / -staging). Файл артефакта — <out-dir>/<uid>.json:
//
//	cd grafana && go run .                                     # оба дашборда в ../dashboard
//	cd grafana && go run . -dashboard key-rate-publications    # только один дашборд
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/cog/plugins"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

func main() {
	which := flag.String("dashboard", "all", "какой дашборд собрать: uid (gold-thesis, key-rate-publications) или all")
	outDir := flag.String("out-dir", "../dashboard", "каталог артефактов: в него пишется <uid>.json")
	flag.Parse()

	// Упорядоченный слайс, а не map: итерация map в Go недетерминирована, а на детерминизме
	// генерации стоит CI-проверка diff артефактов.
	dashboards := []struct {
		uid   string
		build func() *dashboard.DashboardBuilder
	}{
		{GoldThesisUID, NewGoldThesis},
		{KeyRatePublicationsUID, NewKeyRatePublications},
	}

	selected := dashboards
	if *which != "all" {
		selected = nil
		for _, spec := range dashboards {
			if spec.uid == *which {
				selected = append(selected, spec)
			}
		}
		if len(selected) == 0 {
			fail(fmt.Errorf("неизвестный дашборд %q: ожидался uid из списка или all", *which))
		}
	}

	// Нужно, чтобы cog корректно разбирал панели и dataquery известных плагинов.
	plugins.RegisterDefaultPlugins()
	// Регистрируем вариант запроса ClickHouse: по нему cog выбирает CHQuery при разборе JSON.
	cog.NewRuntime().RegisterDataqueryVariant(CHQueryVariant())

	for _, spec := range selected {
		board, err := spec.build().Build()
		if err != nil {
			fail(err)
		}
		if err := Validate(board, spec.uid); err != nil {
			fail(err)
		}

		manifest, err := json.MarshalIndent(Manifest(board), "", "  ")
		if err != nil {
			fail(err)
		}
		out := filepath.Join(*outDir, spec.uid+".json")
		if err := os.WriteFile(out, append(manifest, '\n'), 0o644); err != nil {
			fail(err)
		}
		fmt.Printf("записан %s\n", out)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "grafana: %v\n", err)
	os.Exit(1)
}
