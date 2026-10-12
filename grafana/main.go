// Command grafana генерирует артефакты Grafana как код.
//
// Сборка дашборда gold-thesis (§4 золотого роадмапа) в K8s-стиль манифест,
// который деплоится `gcx resources push` (make grafana-deploy-test / -staging):
//
//	cd grafana && go run . -out ../dashboard/gold-thesis.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/cog/plugins"
)

func main() {
	out := flag.String("out", "../dashboard/gold-thesis.json", "путь к файлу манифеста дашборда")
	flag.Parse()

	// Нужно, чтобы cog корректно разбирал панели и dataquery известных плагинов.
	plugins.RegisterDefaultPlugins()
	// Регистрируем вариант запроса ClickHouse: по нему cog выбирает CHQuery при разборе JSON.
	cog.NewRuntime().RegisterDataqueryVariant(CHQueryVariant())

	board, err := NewGoldThesis().Build()
	if err != nil {
		fail(err)
	}
	if err := Validate(board); err != nil {
		fail(err)
	}

	manifest, err := json.MarshalIndent(Manifest(board), "", "  ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, append(manifest, '\n'), 0o644); err != nil {
		fail(err)
	}

	fmt.Printf("записан %s\n", *out)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "grafana: %v\n", err)
	os.Exit(1)
}
