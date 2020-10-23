SELECT 'monkey', o1.cntr AS id1, o2.cntr AS id2,
  DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) AS d
FROM wise_01.allwise_p3as_psd o1, wise_01.allwise_p3as_psd o2
WHERE CONTAINS(POINT('ICRS', o1.ra, o1.dec), CIRCLE('ICRS', {{ ra }}, {{ dec }}, {{ rsmall }})) = 1
  AND DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) < 0.016
  AND o1.cntr <> o2.cntr
