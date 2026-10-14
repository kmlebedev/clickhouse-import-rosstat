package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"
)

// fp — отпечаток версии в том виде, в каком его даёт util.HeadFingerprint (md5 от заголовков,
// 32 hex-символа), fp16 — те его 16 символов, которые попадают в ключ.
const (
	fp   = "8f3c1d2e4a5b6c7d8e9f0a1b2c3d4e5f"
	fp16 = "8f3c1d2e4a5b6c7d"
)

var zeroSum [sha256.Size]byte

// Отпечаток префлайта есть — ключ строится из него, и sha256 содержимого на ключ не влияет:
// «версия уже в архиве?» на no-op-пути решается одним Exists по данным HEAD, без чтения файла
// (спека §3.2). Если бы содержимое участвовало, проверка (она считает ключ без файла, с нулевой
// суммой) не нашла бы объект, записанный полным импортом, и каждый прогон шёл бы в полный импорт.
func TestKeyFromFingerprint(t *testing.T) {
	got := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum)
	if want := "cbr_m2/" + fp16 + "_monetary_agg_SA.xlsx"; got != want {
		t.Errorf("Key() = %q, ожидалось %q", got, want)
	}
	first := sha256.Sum256([]byte("версия первая"))
	second := sha256.Sum256([]byte("версия вторая"))
	if Key("cbr_m2", "monetary_agg_SA.xlsx", fp, first) != Key("cbr_m2", "monetary_agg_SA.xlsx", fp, second) {
		t.Error("при непустом отпечатке содержимое изменило ключ: префлайт считает ключ без файла")
	}
}

// Отпечатка нет (HEAD недоступен) — ключ из sha256 содержимого. Префикс «sha-» называет
// происхождение префикса (в логе и на Диске он отличим от отпечатка префлайта), а разные версии
// файла обязаны давать разные ключи: иначе новая версия заняла бы место прежней — та самая потеря,
// ради которой архив и заведён (спека §3.1, риск «HEAD недоступен»).
func TestKeyWithoutFingerprintUsesSHA(t *testing.T) {
	sum := sha256.Sum256([]byte("версия"))
	got := Key("cbr_m2", "monetary_agg_SA.xlsx", "", sum)
	want := "cbr_m2/sha-" + hex.EncodeToString(sum[:])[:16] + "_monetary_agg_SA.xlsx"
	if got != want {
		t.Errorf("Key() = %q, ожидалось %q", got, want)
	}
	other := sha256.Sum256([]byte("другая версия"))
	if Key("cbr_m2", "monetary_agg_SA.xlsx", "", other) == got {
		t.Error("разные суммы sha256 дали один ключ: две версии схлопнулись бы в один объект")
	}
}

// Имя приходит из URL источника, а ключ становится путём в хранилище (у LocalStore — путём ФС),
// поэтому разделители и запрещённые в именах символы заменяются. Кириллица сохраняется: выгрузки
// ЦБ и Росстата названы по-русски, и переименование исказило бы аудит версии.
func TestKeySanitizesBasename(t *testing.T) {
	sum := sha256.Sum256([]byte("версия"))
	cases := []struct{ name, basename, want string }{
		{"разделители пути", `monetary/agg\SA.xlsx`, "monetary_agg_SA.xlsx"},
		{"запрещённые в именах", `a:b*c?d"e<f>g|h.xlsx`, "a_b_c_d_e_f_g_h.xlsx"},
		{"управляющие символы", "monetary\x00agg\x1fSA.xlsx", "monetary_agg_SA.xlsx"},
		{"кириллица сохраняется", "выгрузка_ЦБ.xlsx", "выгрузка_ЦБ.xlsx"},
		{"пустое имя", "", "file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Key("cbr_m2", c.basename, fp, sum)
			if want := "cbr_m2/" + fp16 + "_" + c.want; got != want {
				t.Errorf("Key(%q) = %q, ожидалось %q", c.basename, got, want)
			}
		})
	}

	// Обрезка длинного имени: компонент ключа обязан влезать в 255 байт (предел имени файла и на
	// Диске, и в ФС), поэтому имя режется до 200 — префикс версии (≤20) и '_' оставляют запас.
	// Режется по границе руны: basename здесь — 206 байт (1 на «a» + 200 на сто «я» + 5 на «.xlsx»),
	// и обрезка даёт 199 байт, а не 200, где последняя «я» была бы разрезана пополам и имя в
	// хранилище стало бы битым UTF-8.
	t.Run("длинное имя режется по границе руны", func(t *testing.T) {
		basename := "a" + strings.Repeat("я", 100) + ".xlsx"
		got := Key("cbr_m2", basename, fp, sum)
		if want := "cbr_m2/" + fp16 + "_a" + strings.Repeat("я", 99); got != want {
			t.Errorf("Key() = %q, ожидалось %q", got, want)
		}
		component := strings.TrimPrefix(got, "cbr_m2/")
		if !utf8.ValidString(component) {
			t.Errorf("компонент ключа %q — не валидный UTF-8", component)
		}
		if len(component) > 255 {
			t.Errorf("компонент ключа %d байт, предел — 255", len(component))
		}
	})
}

// Короткий отпечаток нештатен (HeadFingerprint всегда даёт 32 hex), но паниковать и отдавать
// пустой префикс нельзя: паника уронила бы импорт на ровном месте, а пустой префикс склеил бы
// версии одного файла в один объект («…/_file.xlsx»).
func TestKeyFingerprintShorterThan16(t *testing.T) {
	for _, c := range []struct{ fingerprint, want string }{
		{"abc", "cbr_m2/abc_x.xlsx"},
		{"f", "cbr_m2/f_x.xlsx"},
	} {
		got := Key("cbr_m2", "x.xlsx", c.fingerprint, sha256.Sum256(nil))
		if got != c.want {
			t.Errorf("Key(отпечаток %q) = %q, ожидалось %q", c.fingerprint, got, c.want)
		}
	}
}
