SELECT 'monkey', ra, dec FROM wise_01.allwise_p3as_mep WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {{ ra }}, {{ dec }}, {{ r1 }})) = 1
