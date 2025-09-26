# Plataforma Distribuída de Processamento de Tarefas

Este projeto implementa uma plataforma distribuída com:
- **Orquestrador Principal** (coordena workers e distribui tarefas);
- **Orquestrador Backup** (recebe estado via multicast e assume em caso de falha);
- **Workers** (executam tarefas e enviam heartbeat);
- **Clientes** (autenticam, submetem tarefas e consultam status).

## Requisitos
- Python 3.9+
- Não há dependências externas (usa apenas bibliotecas padrão)

## Diagramas UML
- Instale a extensão PlantUML.
- Abra o arquivo .puml.
- Pressione Alt+D para visualizar o diagrama.
- Exporte como PNG.
