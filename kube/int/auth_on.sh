kubectl delete ingress tap-ingress -n dax-int
kubectl create -f auth-ingress.yaml -n dax-int
