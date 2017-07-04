apiVersion: v1
kind: Route
metadata:
  name: {{ customconfig['name'] }}
  namespace: {{ customconfig['namespace'] }}
  selfLink: /oapi/v1/namespaces/{{ customconfig['namespace'] }}/routes/{{ customconfig['name'] }}
spec:
  host: {{ customconfig['django_server_external_address'] }}
  to:
    kind: Service
    name: {{ customconfig['name'] }}
    weight: 100
  port:
    targetPort: {{ customconfig['target_port_name'] }}
  wildcardPolicy: None
