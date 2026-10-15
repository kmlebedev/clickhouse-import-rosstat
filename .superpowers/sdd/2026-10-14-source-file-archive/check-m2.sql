SELECT source, name, fingerprint, points, url, first_seen
FROM source_publications FINAL
WHERE source = 'cbr_m2'
ORDER BY first_seen DESC
LIMIT 3;
