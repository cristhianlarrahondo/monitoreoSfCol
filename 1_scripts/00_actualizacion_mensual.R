#  ╔═══════════════════════════════════════════════════════════════════════════╗
#   BBVA Research
#   Autor: Cristhian Larrahondo
#   Fecha: Agosto 2025                                                      
# ╚════════════════════════════════════════════════════════════════════════════╝

# ARCHIVO PRINCIPAL Y DE CONTROL

## ¿QUÉ HACE ESTE CÓDIGO?

# PRIMER BLOQUE
## Permite la descarga mes a mes de cada uno de los archivos que actualiza la 
## información histórica de:
## 1. Crédito y recursos
## 2. Tasas de interés y desembolsos
## 3. Distribución de cartera

# SEGUNDO BLOQUE
## Permite la actualización de las tablas de OUTPUT
## 1. Las de cartera y recursos (carteras)
## 

# == 0. Librerias ==============================================================
{
  library(tidyverse)
  library(glue)
  library(httr)
  library(jsonlite)
  library(lubridate)
  library(fs)
}

rm(list = ls())

# === 1. ACTUALIZACIÓN MES A MES ===============================================

## ---- 1.1 Cargando funciones auxiliares --------------------------------------

source("1_scripts/pull_data.R")
source("1_scripts/pull_data_week.R")
source("1_scripts/cr_data_transform_aux.R")
source("1_scripts/td_data_transform_aux.R")
source("1_scripts/td_data_transform_week.R")
source("1_scripts/dc_data_transform_aux.R")

## ---- 1.2 Definiendo conexión con la API -------------------------------------

# URLs

## Token
api_token = "ksrkPBZQyuodkKKSyDFggj3BG"

## CR
url_cr = "https://www.datos.gov.co/resource/mxk5-ce6w.json?"

## Tasas y desembolsos
url_td = "https://www.datos.gov.co/resource/qzsc-9esp.json?"

# Histórico
url_td_historico = "https://www.datos.gov.co/resource/w9zh-vetq.json?"

# Distribución de cartera
#url_dc = "https://www.datos.gov.co/api/v3/views/rvii-eis8/query.json?"
url_dc = "https://www.datos.gov.co/resource/rvii-eis8.json?"
## ---- 1.3 Actualización para cada dato ---------------------------------------

### --- 1.3.1 Crédito y recursos -----------------------------------------------

#### Trayendo dato mes a mes desde la API
cr_data = pull_data(
  url = url_cr, 
  year = 2026, 
  month = 1, 
  api_token = api_token,
  type = "cr",
  save = "y"
)

#### Actualizando el mes total con el nuevo dato
cr_today = cr_function(data = cr_data)

### ---- 1.3.2 Tasas y desembolsos ---------------------------------------------

#### Trayendo dato mes a mes desde la API
td_data = pull_data(
  url = url_td, 
  year = 2026, 
  month = 2, 
  api_token = api_token, 
  type = "td",
  save = "y"
)

#### Actualizando el mes total con el nuevo dato
# Si no hay mensaje de alerta en la línea anterior, ejectuar esta para 
# actualizar el histórico
td_hoy = td_function(data = td_data)

### --- 1.3.3 Distribución de cartera ------------------------------------------

#### Trayendo dato completo desde la API
dc_data = pull_data(
  url = url_dc, 
  api_token = api_token,
  type = "dc", 
  save = "y"
)

#### Actualizando el mes total con el nuevo dato
dc_today = dc_function(data = dc_data)

# === 2. GENERACIÓN DE OUTPUT ==================================================

source("1_scripts/actualizacion_output.R")

## ---- 2.1 Output - Cartera y recursos ----------------------------------------

### --- 2.1.1 Actualización leasing habitacional -------------------------------
source("1_scripts/output_leasing.R")

### --- 2.1.2 Generación del output --------------------------------------------
source("1_scripts/cr_output.R")

## ---- 2.2 Output - Tasas y desembolsos ----------------------------------------


df <- pull_data_week(
  #url = url_td_historico,
  url = url_td,
  start_date = "2026-02-01", # 05 dic to 
  end_date   = "2026-02-28", # 13 feb
  api_token  = api_token,
  file_name  = "td_feb.rds"
)

td_week = td_function_week(
  data = data, 
  filename = "feb26"
)

load("0_data/2_final/request/feb26.Rdata")

cristhiansPackage::xlsx_exp(
  data = list(td_agregada_m, td_mod_agregada_m),
  sheetnames = c("Subproducto", "Modalidad"), 
  filename = "exc_feb26", 
  path = "0_data/2_final/request"
)
