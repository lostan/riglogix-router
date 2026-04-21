# Classification Rules

## Objetivo
Consolidar os critérios de classificação por produto para uso no pipeline de avaliação de notícias.

## Instruções gerais
- O sistema deve sugerir hipóteses, não tomar decisão final.
- O sistema deve sempre explicar o racional.
- O sistema deve separar fit técnico de janela de oportunidade.
- O sistema deve apontar incertezas quando faltarem dados relevantes.

## Lógica global de score
### Score de fit por produto
Escala sugerida:
- 0 = sem aderência
- 1 = aderência fraca
- 2 = aderência moderada
- 3 = aderência boa
- 4 = aderência forte
- 5 = aderência muito forte

### Score de timing
Escala sugerida:
- 0 = timing ruim
- 1 = cedo demais
- 2 = possível, mas incerto
- 3 = timing razoável
- 4 = timing bom
- 5 = timing muito bom

## Regras por produto

### [NOME DO PRODUTO]
#### Critérios obrigatórios
- 
- 

#### Critérios fortes
- 
- 

#### Critérios complementares
- 
- 

#### Fatores de despriorização
- 
- 

#### Exemplo de racional positivo
- 

#### Exemplo de racional negativo
- 

## Regras de priorização adicional
- aumentar prioridade se conta-alvo
- aumentar prioridade se geografia foco
- aumentar prioridade se vendedor tiver foco explícito no produto
- reduzir prioridade se notícia estiver fora do escopo atual do vendedor

## Saída esperada
Para cada notícia, gerar:
- produtos sugeridos
- score por produto
- leitura de timing
- racional
- incertezas
- recomendação de monitorar ou aprofundar análise