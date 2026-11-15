# #490: bounded census source contract

**GO for the central regional snapshot; ITERATE for municipalities.** Re-fetched official `Tom2_tab1_VPN-2020.xlsx` and `Tom2_tab2_VPN-2020.xlsx`: actual HTTP200, valid ZIP/XLSX,38362/632954bytes. URL/UTC/status/content-type/hash in `census-2020/source-probes.json`. Explicit normalized mapping is `census-2020/territories.json`. The research subtask performed no repository edits, imports or DB writes; implementation and live acceptance are documented separately in the #490 plan.

Reference date is **2021-10-01**, despite the census name VPN2020 (official landing statement verified in#612). Source table2 sheet «таб 2»: B=both sexes,C=male,D=female, persons. Import scope proposed by parent: all ages +18 mutually exclusive bins0–4,…,80–84,85+, actual settlement blocks only. Do not import mean/median, overlapping groups, working-age or E–H percentages/ratios.

**96 source territories:**1 RF,8 federal districts,83 subjects,2 composite subjects including AO (Архангельская область/Тюменская область),2 remainders excluding AO. Thus85 subject entities exist if composites count as entities; disjoint geographic aggregation instead uses83 subjects +2 exclAO, omitting composites. `parent_FD` and source header/settlement row numbers are explicit in mapping. Whitespace collapsed only; preserve source dash characters. No name guessing or fuzzy level detection needed in production.

**Moscow and StPetersburg are urban-only:** source headers rows1904/3138, only an urban block follows. Other94 territories each have total/urban/rural. Observed blocks=284, not288; target count cells=284×19ages×3sex=**16188**. No selected blank/dash/text values; every targeted count is an explicit nonnegative integer. No unknown-age row in this snapshot; each18-bin partition fully exhausts its all-age total. Future unknown-age, unexpected text or changed layout should fail closed, not disappear silently. X appears in non-count mean/median ratio cells; exclude those cells. Absence of Moscow/SPb total/rural is structural, not a blank numeric cell. Do not synthesize rural zero or duplicate urban as total unless separately defined as derived.

Controls passed across the entire targeted source:

- 5396 row sex identities: both=male+female.
- 284 settlement blocks ×3 sex:18 bins sum to all-age total.
- 5358 settlement identities: total=urban+rural over94 territories ×19 ages ×3 sex.
- 570 geographic comparisons across19 ages ×3 sex: RF=sum8FD; RF=sum85 disjoint parts; eachFD=sum its disjoint parts.
- RF all-age total147182123=68431580male+78750543female; table1 same independent workbook total. For geographic controls only, Moscow/SPb values come from their observed urban block; no synthetic point is created.

Municipal machine-readable data cannot be declared absent. Two concrete official listing targets from primary-source search index:

1. https://16.rosstat.gov.ru/vpn2020 — Том2/Таблица2 «Население по возрасту и полу по городским округам и муниципальным районам Республики Татарстан», XLSX207.65KB,13.03.2023. Actual listing GET timed out20s; no direct workbook href/bytes verified.
2. https://72.rosstat.gov.ru/perepis_nas2020 — Том2/Таблица2 «Население по возрастным группам и полу по муниципальным образованиям»: XLSX60.03KB(ХМАО),40.91KB(ЯНАО),68.37KB(Тюменскаяобласть),26.05.2023. Indexed official listing metadata only; direct GET not attempted after bounded timeout budget.

Additional official listings21.rosstat.gov.ru/folder/215047 and59.rosstat.gov.ru/folder/200657 each timed out25s. Timeout means unverified availability, not no source. Exact file URLs remain unresolved; do not invent them from filenames. Named followup `municipal-census-age-sex` should resolve these specific table links, verify actual bytes/code system/reference date and numeric municipal controls before proposing nationwide municipal import. Current implementation may be explicitly partial (regions only), as issue490 allows, without claiming municipality machine sources do not exist.

Public access is proven for central files; open license, historical PIT and long-term stability are not. Preserve URL/hash/observed_at separately from reference_date. Last-Modified is not publication date. Table1 is an independent numeric control, not a second permanent import source.

Implementation decision: bounded regional snapshot in #490; municipalities remain explicitly tracked as [rosstat/1.15 #617](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/617). No claim of nationwide municipal completeness.
