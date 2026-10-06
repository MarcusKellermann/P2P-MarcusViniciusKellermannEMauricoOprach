# Sistema P2P de sincronizacao de arquivos
# Feito por Marcus Vinicius Kellermann e Mauricio Oprach

Trabalho academico de sincronizacao distribuida de arquivos entre peers usando UDP. Cada peer monitora seu diretorio local, anuncia arquivos novos ou alterados e propaga remocoes para os demais peers configurados.

## Requisitos

- Python 3.10 ou superior
- Nenhuma dependencia externa para executar `p2p.py`
- Conectividade UDP entre os peers

## Como executar

Abra um terminal para cada peer. Cada processo permanece em execucao enquanto escuta a rede e monitora seu diretorio. Os diretorios sao criados automaticamente se ainda nao existirem.

No mesmo computador, use `127.0.0.1` e uma porta diferente para cada peer. Execute os comandos a partir da pasta do projeto.

### Peer 1

```powershell
python .\p2p.py --host 127.0.0.1 --port 5001 --peer 127.0.0.1:5002 --peer 127.0.0.1:5003 --dir .\tmp1
```

### Peer 2

```powershell
python .\p2p.py --host 127.0.0.1 --port 5002 --peer 127.0.0.1:5001 --peer 127.0.0.1:5003 --dir .\tmp2
```

### Peer 3

```powershell
python .\p2p.py --host 127.0.0.1 --port 5003 --peer 127.0.0.1:5001 --peer 127.0.0.1:5002 --dir .\tmp3
```

Para adicionar outro peer, escolha uma porta livre e configure os enderecos dos peers existentes. Por exemplo:

```powershell
python .\p2p.py --host 127.0.0.1 --port 5004 --peer 127.0.0.1:5001 --peer 127.0.0.1:5002 --peer 127.0.0.1:5003 --dir .\tmp4
```

Para encerrar um peer, pressione `Ctrl+C` no terminal correspondente.

## Teste de sincronizacao

Com os peers ativos, crie ou altere um arquivo no diretorio de um deles:

```powershell
Set-Content .\tmp1\relatorio.txt "teste de sincronizacao"
```

O arquivo deve aparecer nos diretorios dos outros peers. Para testar a remocao:

```powershell
Remove-Item .\tmp1\relatorio.txt
```

A remocao tambem deve ser propagada. O peer verifica os arquivos periodicamente; aguarde alguns segundos para observar as mudancas.

## Como funciona

- Os peers sao configurados manualmente com `--peer HOST:PORTA`.
- Mensagens de descoberta, lista, heartbeat e atualizacao usam UDP.
- Arquivos sao divididos em blocos para transferencia e verificados com SHA-256.
- Cada peer utiliza um diretorio independente indicado por `--dir`.
- O processo exibe periodicamente os peers ativos e os arquivos conhecidos.

## Execucao em computadores diferentes

Substitua `127.0.0.1` pelo endereco IP acessivel de cada computador. Configure em cada peer a lista dos outros enderecos e portas, permita o trafego UDP no firewall e confirme que as maquinas estao na mesma rede ou possuem conectividade entre si. `127.0.0.1` so funciona para processos na mesma maquina.

Depois de reiniciar um peer, os peers configurados trocam novamente suas listas de arquivos para recuperar a sincronizacao.

## Cliente TCP complementar

O arquivo `client.py` contem uma implementacao cliente TCP separada do peer UDP acima. Ele monitora um diretorio e troca mensagens com um servidor TCP compativel. Para usar esse cliente, instale a dependencia:

```powershell
python -m pip install watchdog
```

O cliente recebe o endereco e a porta do servidor e, opcionalmente, o diretorio compartilhado:

```powershell
python .\client.py --host 127.0.0.1 --port 6001 --dir .\tmp_cliente
```

Este repositorio nao inclui um servidor TCP compativel com `client.py`; portanto, esse comando so conecta quando tal servidor estiver em execucao no endereco e porta informados. O cliente TCP nao se conecta diretamente aos peers UDP iniciados com `p2p.py`.
