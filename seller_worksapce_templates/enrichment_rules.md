# Enrichment Rules

## Objetivo
Definir quais informações devem ser buscadas ou inferidas para enriquecer notícias antes da classificação.

## Princípios
- Enriquecer apenas o que melhora a decisão.
- Não inventar informação ausente.
- Registrar incerteza explicitamente.
- Priorizar campos que impactam fit e timing.

## Campos prioritários de enriquecimento
- profundidade d'água
- condições ambientais
- número de poços
- início estimado da campanha
- fase do projeto
- tipo de rig esperado
- drilling contractor
- status da conta
- experiência prévia com conta, rig ou contractor

## Regras por tipo de dado

### Profundidade d'água
- Por que importa:
- Como usar:
- Quando afeta score:

### Condições ambientais
- Por que importa:
- Como usar:
- Quando afeta score:

### Fase do projeto
- Por que importa:
- Como inferir:
- Quando afeta timing:

### Rig / drillship
- Por que importa:
- Como usar:
- Quando aumenta prioridade:

### Drilling contractor
- Por que importa:
- Como usar:
- Quando aumenta prioridade:

## Fontes de inferência permitidas
- notícia original
- base da conta
- base do produto
- contexto conhecido do vendedor
- outras fontes externas autorizadas

## Saída esperada do enriquecimento
Cada notícia enriquecida deve conter:
- campos confirmados
- campos inferidos
- nível de confiança
- lacunas ainda abertas