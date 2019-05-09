SELECT 'monkey', o1.id AS id1, o2.id AS id2,
  DISTANCE(POINT('ICRS', o1.coord_ra, o1.coord_decl), POINT('ICRS', o2.coord_ra, o2.coord_decl)) AS d
FROM sdss_stripe82_01.RunDeepSource o1, sdss_stripe82_01.RunDeepSource o2
WHERE CONTAINS(POINT('ICRS', o1.coord_ra, o1.coord_decl), CIRCLE('ICRS', {{ ra }}, {{ dec }}, {{ r1 }})) = 1
  AND DISTANCE(POINT('ICRS', o1.coord_ra, o1.coord_decl), POINT('ICRS', o2.coord_ra, o2.coord_decl)) < 0.000277778
  AND o1.id <> o2.id
