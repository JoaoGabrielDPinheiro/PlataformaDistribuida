# Plataforma Distribuída de Processamento Colaborativo de Tarefas

Este projeto implementa uma plataforma distribuída em Python para processamento colaborativo de tarefas, com suporte a **balanceamento de carga**, **tolerância a falhas** e **failover automático do orquestrador**.

## Estrutura do Projeto

```
PlataformaDistribuida/
├─ src/
│  ├─ orchestrator.py   # Orquestrador principal
│  ├─ backup.py         # Orquestrador de backup (failover)
│  ├─ worker.py         # Worker (execução de tarefas)
│  ├─ client.py         # Cliente (submissão e consulta de tarefas)
│  ├─ logger_setup.py   # Configuração de logs
├─ config/
│  ├─ settings.py       # Configurações globais
├─ logs/                # Diretório para registros de execução
└─ state_checkpoint.json # Arquivo de checkpoint
```

## Funcionalidades

* Registro de **workers** no orquestrador.
* Balanceamento de carga baseado em **Least Load**.
* Detecção de falhas por **heartbeat**.
* Redistribuição de tarefas em caso de falha de worker.
* **Failover automático** para o backup se o orquestrador principal cair.
* Persistência de estado em **checkpoints**.
* Logs detalhados de execução.

## Como Executar

Abra múltiplos terminais a partir da pasta raiz (`PlataformaDistribuida`):

1. Inicie o **orquestrador principal**:

   ```
   python -m src.orchestrator
   ```

2. Inicie o **backup**:

   ```
   python -m src.backup
   ```

3. Inicie um ou mais **workers** (porta diferente para cada um):

   ```
   python -m src.worker --id W1 --port 6001
   python -m src.worker --id W2 --port 6002
   ```

4. Execute o **cliente**:

   ```
   python -m src.client
   ```

No cliente, é possível autenticar, submeter tarefas e consultar resultados.

## Testes

* **Execução básica**: submeter uma tarefa simples e verificar resultado no worker.
* **Balanceamento**: iniciar múltiplos workers e observar a distribuição das tarefas.
* **Falha de worker**: submeter tarefa com `"crash": True` e verificar redistribuição.
* **Failover**: encerrar o orquestrador principal e verificar o backup assumindo.

## Requisitos

* Python 3.9+
* Sistema operacional com suporte a sockets TCP/UDP
* Execução em ambiente local ou em rede
* Extensão PlantUML.
