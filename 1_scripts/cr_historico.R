################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Histórico IG 
# Consolida todos los archivos de indicadores gerenciales en uno solo agregado a
# a nivel total y por bancos
# Tiene las variables de:
# Recursos: vista y depósitos
# Carteras: bruta, vigente y vencida

# Nota: Este archivo no debe cambiar, se consolida una sola vez y funciona como
# ancla para ir incluyendo las actualizaciones

################################################################################
# Librerias
{
  library(tidyverse)
  library(readxl)
  library(glue)
  library(purrr)
  library(stringi)
}

rm(list = ls())

################################################################################

# Archivos principales y auxiliares

## Ubicación archivo indicadores gerenciales
file_path = "0_data/0_raw/IG.xlsx"

## Listado nombres de entidades para estandarizar
entidades_ids = read_excel(path = "0_data/0_raw/diccionarios/cr_listado_entidades.xlsx")

## Nombres de variables para estandarizar
codes = read_excel(
  path = "0_data/0_raw/diccionarios/diccionario_cr.xlsx",
  sheet = "historical"
)

################################################################################

# Auxiliares para formato de archivo

## Nombres de las filas (variables) seleccionadas del archivo IG
row_names = c(
  "recursos_vista_corriente",
  "recursos_vista_ahorro",
  "recursos_termino",
  "cartera_total",
  "cartera_vigente_total",
  "cartera_vencida_total",
  "cartera_vigente_comercial",
  "cartera_comercial",
  "cartera_vencida_comercial",
  "cartera_vigente_consumo",
  "cartera_consumo",
  "cartera_vencida_consumo",
  "cartera_vigente_hipotecaria",
  "cartera_hipotecaria",
  "cartera_vencida_hipotecaria",
  "cartera_vigente_microcredito",
  "cartera_microcredito",
  "cartera_vencida_microcredito"
)

## Nombres de todas las hojas del archivo IG (mes-año)
sheet_names = excel_sheets(file_path)#[201:267]

################################################################################

# Función para procesar cada hoja de excel (por mes)
process_sheet = function(sheet) {
  
  ##############################################################################
  # Lee el archivo en la hoja indicada
  file = read_excel(path = file_path, sheet = sheet)
  
  # Extrae el año de cada hoja
  year = as.numeric(str_extract(sheet, "\\d{2}"))
  ## Ajusta el año para que sea formato YYYY
  year = ifelse(year >= 90, 1900 + year, 2000 + year)
  
  # Fila donde inicia el archivo
  row_to_start = min(which(str_detect(
    tolower(unlist(file[, 4])),  # Convert to lowercase
    "\\btotal\\b"                # Match "total" as a whole word
  )))
  
  # Columna donde inicia el archivo
  column_banco_start = min(which(str_detect(file[row_to_start, ], "BANCO")))
  
  ##############################################################################
  
  # Indica las filas donde está la información de recursos
  ## Es diferente según el año (el formato del archivo cambió)
  if (year <= 2014) { # Para los años menores o iguales a 2014
    
    # Punto de partida de recursos
    recursos_guide = min(which(str_detect(file[[3]], "DEPOSITOS Y EXIGIBILIDADES")))
    ## Vista - corriente
    row_vista_corriente = recursos_guide + 1
    ## Vista - ahorro
    row_vista_ahorro = recursos_guide + 2
    ## Depósito
    row_termino = recursos_guide + 3
    
    # Juntando las filas
    row_recursos = c(
      row_vista_corriente, row_vista_ahorro, row_termino
    )
    
  } else { # Para los años mayores a 2014
    
    ## Vista - corriente
    row_vista_corriente = min(which(str_detect(file[[3]], "DEPÓSITOS EN CUENTA CORRIENTE")))
    ## Vista - ahorro
    row_vista_ahorro = min(which(str_detect(file[[3]], "DEPÓSITOS DE AHORRO")))
    row_termino = min(which(str_detect(file[[3]], "CERTIFICADOS DE DEPÓSITO A TERMINO")))
    
    # Juntando las filas
    row_recursos = c(
      row_vista_corriente, row_vista_ahorro, row_termino
    )
    
  }
  
  # Carteras
  ## Total
  row_cartera_total = min(which(str_detect(file[[2]], "TOTAL CARTERA")))
  ## Comercial
  row_cartera_comercial = min(which(str_detect(file[[3]], "TOTAL COMERCIAL")))
  ## Consumo
  row_cartera_consumo = min(which(str_detect(file[[3]], "TOTAL CONSUMO")))
  ## Hipotecaria
  row_cartera_vivienda = ifelse(
    # Antes de 2003 se le llamaba hipotecaria
    test = year <= 2003,
    min(which(str_detect(file[[3]], "TOTAL HIPOTECARIA"))),
    # Luego de 2003 la llaman vivienda
    min(which(str_detect(file[[3]], "TOTAL VIVIENDA")))
  )
  ## Microcrédito
  row_cartera_microcredito = min(which(str_detect(file[[3]], "TOTAL MICROCREDITO")))
  
  # Juntando todas las carteras
  rows_carteras = c(
    row_cartera_total + 1, row_cartera_total + 2, row_cartera_total + 3,
    row_cartera_comercial + 1, row_cartera_comercial + 6, row_cartera_comercial + 7,
    row_cartera_consumo + 1, row_cartera_consumo + 6, row_cartera_consumo + 7,
    row_cartera_vivienda + 1, row_cartera_vivienda + 8, row_cartera_vivienda + 9,
    row_cartera_microcredito + 1, row_cartera_microcredito + 6, row_cartera_microcredito + 7
  )
  ##############################################################################
  
  ## Primera columna 
  first_na_col = which(is.na(file[row_to_start, column_banco_start:ncol(file)]))[1]
  ## Última columna
  last_col = ifelse(test = is.na(first_na_col), ncol(file), column_banco_start + first_na_col - 2)
  
  ##############################################################################
  B =
  # Transformando toda la data
  file %>%
    # Seleccionando columnas de interés
    dplyr::select(3, 4, column_banco_start:last_col) %>% 
    # Seleccionando filas de interés
    slice(c(row_to_start, row_recursos, rows_carteras)) %>% 
    # Filas a nombres de columnas
    janitor::row_to_names(row_number = 1, remove_row = T) %>%
    # Asignando a la primera columna los nombres de las variables
    mutate(across(1, ~ row_names)) %>% 
    # Todas las columnas excepto la primera como numéricas
    mutate(across(-1, as.numeric)) %>% 
    # A formato long
    pivot_longer(
      cols = 2:(dplyr::last_col()),
      names_to = "entidad",
      values_to = "values"
    ) %>%
    # Agregando los nombres de las entidades
    left_join(entidades_ids, by = join_by(entidad)) %>%
    # Renombrando la columna total
    mutate(name = ifelse(
      str_detect(
        tolower(entidad),
        "(?=.*total)(?=.*sin)(?=.*ioe)|(?=.*total)(?=.*entidad)"  # Match either condition
      ),
      "Total",
      name
    )) %>% 
    dplyr::select(-entidad) %>%
    # Removiendo duplicados
    dplyr::distinct() %>%
    # Ahora a formato wide
    pivot_wider(names_from = name, values_from = values) %>%
    rename_with(~ "variable", .cols = 1) %>% 
    # Creando la variable fecha
    mutate(fecha = sheet) %>% 
    # Reubicando
    relocate(fecha, .after = variable)
}

################################################################################

# Aplicando la función para crear una lista de dataframes por cada hoja del excel
dt_list = map(sheet_names, process_sheet)
# De lista a un solo dataframe
final_dt = bind_rows(dt_list)

################################################################################

# Dataframe final para guardar
historical_dec24 = final_dt %>% 
  mutate(
    fecha = tolower(fecha),  # Convert to lowercase to avoid case issues
    fecha = gsub("ene", "01", fecha),
    fecha = gsub("feb", "02", fecha),
    fecha = gsub("mar", "03", fecha),
    fecha = gsub("abr", "04", fecha),
    fecha = gsub("may", "05", fecha),
    fecha = gsub("jun", "06", fecha),
    fecha = gsub("jul", "07", fecha),
    fecha = gsub("ago", "08", fecha),
    fecha = gsub("sep", "09", fecha),
    fecha = gsub("oct", "10", fecha),
    fecha = gsub("nov", "11", fecha),
    fecha = gsub("dic", "12", fecha),
    fecha = parse_date_time(fecha, "my")  # Convert to date format
  ) %>% 
  # Nombre de varaibles a homogenizar para utilizar con los de la API
  left_join(codes, by = join_by(variable)) %>% 
  # Reubicando columnas
  relocate(tipo, macrocuenta, .before = fecha) %>% 
  pivot_longer(
    cols = -c(1:4), 
    names_to = "nombre_entidad", 
    values_to = "valor"
  ) %>% 
  arrange(variable, fecha) %>% 
  dplyr::select(-variable) %>% 
  group_by(tipo, macrocuenta, fecha, nombre_entidad) %>%
  reframe(
    valor = sum(valor, na.rm = T)
  ) %>% ungroup() %>% 
  filter(tipo != "Vigente") %>% 
  pivot_wider(names_from = c(tipo, macrocuenta), values_from = valor) %>% 
  mutate(
    Recursos_Total = Recursos_Vista + Recursos_CDT
  ) %>% 
  pivot_longer(
    cols = -c(fecha:nombre_entidad), 
    names_to = c("tipo", "macrocuenta"), 
    names_sep = "_",
    values_to = "valor", 
    values_drop_na = T
  ) %>% 
  arrange(fecha, tipo, macrocuenta, nombre_entidad)

################################################################################

# Cartera
dt_cartera = read_excel(
  path = "0_data/0_raw/cr_historico/Depositos y cartera1.xlsx", 
  sheet = "Cartera", 
  range = "A4:O369", 
  col_names = FALSE
) %>% 
  dplyr::select(1, 4:11, 14, 15) %>% 
  setNames(., c(
    "fecha", 
    "Comercial_Bruta", "Comercial_Vencida",
    "Consumo_Bruta", "Consumo_Vencida",
    "Hipotecaria_Bruta", "Hipotecaria_Vencida",
    "Microcrédito_Bruta", "Microcrédito_Vencida",
    "Total_Bruta", "Total_Vencida"
  )) %>% 
  pivot_longer(
    cols = 2:11,
    names_to = c("macrocuenta", "tipo"),
    names_sep = "_",
    values_to = "valor", 
    values_drop_na = T
  ) %>% 
  mutate(
    nombre_entidad = "Total",
    valor = valor * 1000
  ) %>% 
  filter(
    fecha <= "2024-12-01"
  )

# Depósitos
dt_depositos = read_excel(
  path = "0_data/0_raw/cr_historico/Depositos y cartera1.xlsx", 
  sheet = "Depositos", 
  range = "A5:F366", 
  col_names = FALSE
) %>% 
  dplyr::select(1, 4:6) %>% 
  setNames(., c(
    "fecha", 
    "Vista_Recursos", "CDT_Recursos", "Total_Recursos"
  )) %>% 
  filter(
    fecha <= "2024-12-01"
  ) %>% 
  pivot_longer(
    cols = 2:4,
    names_to = c("macrocuenta", "tipo"),
    names_sep = "_",
    values_to = "valor", 
    values_drop_na = T
  ) %>% 
  mutate(
    nombre_entidad = "Total"
  )

################################################################################

historical_dec24_v2 = historical_dec24 %>% 
  filter(
    # Dejar por fuera las carteras totales
    nombre_entidad != "Total"
  ) %>% 
  bind_rows(dt_cartera) %>% 
  bind_rows(dt_depositos) %>% 
  mutate(
    fecha = as.Date(floor_date(ymd(fecha), "month"))
  )


# Guardando la versión histórica hasta dic-2024
saveRDS(historical_dec24_v2, "0_data/1_processed/historico/cr/cr_historico_dic24_v2.rds")


historical_dec24_v2 %>% 
  filter(nombre_entidad == "Total") %>% 
  ggplot() +
  geom_line(aes(x = fecha, y = valor)) +
  facet_wrap(tipo ~ macrocuenta, scale = "free_y")
