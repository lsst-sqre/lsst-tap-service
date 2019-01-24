SELECT 'monkey', ra, decl FROM wise_00.allwise_p3as_mep WHERE CONTAINS(POINT('ICRS', ra, decl), BOX('ICRS', {{ ra }}, {{ dec }}, {{ r1 }}, {{ r2 }})) = 1
