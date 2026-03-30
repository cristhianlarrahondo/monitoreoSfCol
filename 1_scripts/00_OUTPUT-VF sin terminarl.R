################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Agosto 2025
################################################################################

# 

################################################################################
# Librerias
{
  library(tidyverse)
}

rm(list = ls())

################################################################################

# Datos ya procesados

## Tasas y desembolsos
# Cargando la data de td
load(file = "0_data/1_processed/mas_reciente/td/td_mas_reciente.RData")

### Fecha para tener de inflación
date_max_td = td_agregada %>% pull(fecha) %>% max()

## Créditos y recursos
cr = readRDS(file = "0_data/1_processed/mas_reciente/cr/cr_mas_reciente.rds") %>% 
  arrange(nombre_entidad, tipo, macrocuenta, fecha)

### Fecha para tener de inflación
date_max_cr = cr %>% pull(fecha) %>% max()

################################################################################
# Datos manuales necesarios para cálculos

# Mostrar mensaje llamativo
cli::cli_alert_info(
  text = paste0(
    crayon::make_style(
      "darkred", bg = T
    )(
      cli::style_bold(
        "No olvide primero actualizar los archivos de: 
        INFLACIÓN Y TITULARIZACIONES \n\n"
      )
    ),
    "< Inflación mínimo al corte: {date_max_td} >\n\n",
    "< Titularizaciones mínimo al corte: {date_max_cr} >\n\n",
    "¿Actualizó el archivo?\n",
    "Si es así, escriba ", crayon::green$bold("y"), ", de lo contrario ", crayon::red$bold("n")
  )
)

# Leer respuesta del usuario
respuesta = tolower(readline(prompt = crayon::yellow$bold("Ingrese su respuesta (y/n): ")))

# Verificar la respuesta
if (respuesta != "y") {
  cli::cli_alert_danger(
    "El proceso se detuvo porque no se han actualizado los archivos necesarios."
  )
  stop("Actualice y vuelva a ejecutar")
  
} else {

    cli::cli_alert_success("¡Gracias! Continuando con el proceso...")

}

################################################################################
## Inflación

cli::cli_alert_success("¡Cargando datos de inflación!")

# Archivo de inflación
file_inflacion = list.files(
  path = "0_data/0_raw/inflacion/", pattern = ".xlsx", full.names = T
)

  # Check sólo un archivo
  if (length(file_inflacion) > 1) {
    cli::cli_alert_danger(
      "Sólo debe haber un archivo en la carpeta de inflación; el más reciente."
    )
    
    stop("Corrija y ejecute de nuevo.")
  } else {
    cli::cli_alert_success("¡Cargando, gracias! Continuando con el proceso...")
  }

# Cargando el archivo
inflacion = readxl::read_excel(
  path = glue::glue("{file_inflacion}"), 
  skip = 1,
  col_names = F
) %>% 
  dplyr::select(1, 2) %>% 
  setNames(., c("fecha", "inflacion"))

  ## Check del mes necesario
  if (max(inflacion$fecha) < date_max_td) {
    cli::cli_alert_danger(
      "
      El mes más reciente de inflación debe ser al menos el disponible en tasas y desembolsos      Encontrado en inflación: {max(inflacion$fecha)}
      Tasas y desembolsos: {date_max_td}
      "
    )
    
    stop("Actualíce y ejecute de nuevo.")
  }

################################################################################
## Titularizaciones

cli::cli_alert_success("¡Cargando datos de titularizaciones!")

# Archivo de titularizaciones
file_titularizaciones = list.files(
  path = "0_data/0_raw/titularizaciones/", pattern = ".xlsx", full.names = T
)

  # Check sólo un archivo
  if (length(file_titularizaciones) > 1) {
    cli::cli_alert_danger(
      "Sólo debe haber un archivo en la carpeta de titularizaciones; el más reciente."
    )
    
    stop("Corrija y ejecute de nuevo.")
  } else {
    cli::cli_alert_success("¡Cargado, gracias! Continuando con el proceso ...")
  }

# Titularizaciones cartera vencida
titu_vencida = readxl::read_excel(
  path = glue::glue("{file_titularizaciones}"), 
  sheet = "CalidadTOTAL", 
  col_names = T,
  skip = 6
) %>%
  dplyr::select(1, 2) %>% 
  setNames(., c("fecha", "npl")) %>% 
  filter(!is.na(npl)) %>% 
  mutate(
    fecha = as.Date(as.numeric(fecha), origin = "1899-12-30"),
    fecha = floor_date(fecha, unit = "month")
  )

# Saldo total titularizaciones (Millones de pesos)
titu = readxl::read_excel(
  path = glue::glue("{file_titularizaciones}"), 
  sheet = "Saldo Total", 
  col_names = F,
  skip = 6,
  col_types = c("date", "numeric")
) %>% 
  setNames(., c("fecha", "titularizaciones")) %>% 
  mutate(
    fecha = floor_date(fecha, unit = "month")
  ) %>% 
  left_join(titu_vencida, by = join_by(fecha)) %>% 
  mutate(
    Vencida = titularizaciones * npl
  ) %>% 
  rename(Bruta = titularizaciones) %>% 
  dplyr::select(-npl) %>% 
  pivot_longer(
    cols = -fecha, 
    names_to = "tipo", values_to = "titularizaciones"
  )

  ## Check del mes necesario
  if (max(titu$fecha) < date_max_cr) {
    cli::cli_alert_danger(
      "
        El mes más reciente de titularizaciones debe ser al menos el disponible en depósitos y carteras
        Encontrado en inflación: {max(inflacion$fecha)}
        Tasas y desembolsos: {date_max_td}
        "
    )
    stop("Actualíce y ejecute de nuevo.")
  } else {
    cli::cli_alert_success("¡Todo en orden! Continuando con el proceso!")
  }

################################################################################
# Datos automáticos necesarios para cálculos
## Leasing habitacional
################################################################################

# Leasing
source("1_scripts/output_leasing.R")
leasing = readRDS(file = "0_data/1_processed/mas_reciente/leasing/leasing_mas_reciente.rds")

################################################################################

# OUTPUT

cli::cli_alert_success("¡Empezando con el proceso de las tablas OUTPUT!")

################################################################################

## Carteras

cli::cli_alert_success("¡Iniciando con CARTERAS!")

cre_total = cr %>% 
  filter(
    nombre_entidad == "Total",
    tipo != "Recursos"
  ) %>% 
  dplyr::select(-nombre_entidad) 

cre_total_titu = cre_total %>% 
  filter(macrocuenta == "Total") %>% 
  left_join(titu, by = join_by(fecha, tipo)) %>% 
  mutate(
    valor = valor + titularizaciones,
    macrocuenta = "Total+Titularizaciones"
  ) %>% 
  dplyr::select(-titularizaciones)

A = cre_total %>% 
  bind_rows(cre_total_titu) %>% 
  bind_rows(leasing) #%>% 

A_w = A %>% 
  mutate(
    # Miles de millones
    valor = valor/1000 
  ) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "valor"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_bruta, comercial_vencida,
    consumo_bruta, consumo_vencida,
    hipotecaria_bruta, hipotecaria_vencida,
    microcredito_bruta, microcredito_vencida,
    total_bruta, total_vencida,
    leasing_bancos_bruta, leasing_cf_bruta,
    total_titularizaciones_bruta, total_titularizaciones_vencida
  )

A_gr = A %>%
  filter(year(fecha) > 2002) %>% 
  arrange(macrocuenta, tipo, fecha) %>% 
  group_by(macrocuenta, tipo) %>% 
  mutate(
    gr = (valor - lag(valor, 12)) / lag(valor, 12) * 100,
  ) %>% 
  dplyr::select(-valor)

A_gr_w = A_gr %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "gr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_bruta, consumo_bruta, hipotecaria_bruta, microcredito_bruta,
    total_bruta,
    leasing_bancos_bruta, leasing_cf_bruta,
    total_titularizaciones_bruta, 
    comercial_vencida, consumo_vencida, hipotecaria_vencida, microcredito_vencida,
    total_vencida, total_titularizaciones_vencida
  ) %>% 
  rename_with(~ paste0("gr_", .), .cols = -fecha)

A_gr_real = A_gr %>% 
  left_join(inflacion, by = join_by(fecha)) %>% 
  mutate(
    rgr = ((1 + gr/100)/(1 + inflacion/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(gr, inflacion)) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "rgr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_bruta, consumo_bruta, hipotecaria_bruta, microcredito_bruta,
    total_bruta,
    leasing_bancos_bruta, leasing_cf_bruta,
    total_titularizaciones_bruta, 
    comercial_vencida, consumo_vencida, hipotecaria_vencida, microcredito_vencida,
    total_vencida, total_titularizaciones_vencida
  ) %>% 
  rename_with(~ paste0("rgr_", .), .cols = -fecha)

A_share = A %>% 
  filter(
    !grepl("Total", macrocuenta)
  ) %>% 
  rename(valor_sub = valor) %>% 
  left_join(
    cre_total %>%
      filter(macrocuenta == "Total") %>%
      dplyr::select(-macrocuenta),
    by = c("fecha", "tipo")
  ) %>% 
  mutate(
    share = valor_sub/valor * 100
  ) %>% 
  dplyr::select(-c(valor_sub, valor)) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "share"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_bruta, consumo_bruta, hipotecaria_bruta, microcredito_bruta,
    leasing_bancos_bruta, leasing_cf_bruta,
    comercial_vencida, consumo_vencida, hipotecaria_vencida, microcredito_vencida
  ) %>% 
  rename_with(~ paste0("sh_", .), .cols = -fecha)

npl = A %>% 
  filter(
    tipo == "Bruta",
    macrocuenta %in% c("Comercial", "Consumo", "Hipotecaria", "Microcrédito", "Total")
  ) %>% 
  rename(bruta = valor) %>% 
  dplyr::select(-tipo) %>% 
  left_join(
    A %>% 
      filter(tipo == "Vencida") %>% 
      rename(vencida = valor) %>% 
      dplyr::select(-tipo),
    by = join_by(fecha, macrocuenta)
  ) %>% 
  mutate(
    npl = vencida/bruta * 100
  ) %>% 
  dplyr::select(fecha, macrocuenta, npl) %>% 
  pivot_wider(
    names_from = macrocuenta, 
    values_from = npl
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial, consumo, hipotecaria, microcredito, total
  ) %>% 
  rename_with(~ paste0("npl_", .), .cols = -fecha)

## Tabla consolidada
tab_A = A_w %>% 
  left_join(A_gr_w,    by = "fecha") %>%
  left_join(A_gr_real, by = "fecha") %>%
  left_join(A_share,   by = "fecha") %>% 
  left_join(inflacion, by = "fecha") %>% 
  left_join(npl, by = "fecha") %>% 
  relocate(inflacion, .after = fecha) %>% 
  mutate(
    fecha = as.Date(fecha)
  ) %>% 
  mutate(across(where(is.numeric), ~ ifelse(
    is.na(.x), NA_character_,
    format(round(.x, 4), nsmall = 4, scientific = FALSE)
  )))

cli::cli_alert_success("¡Exportando el OUTPUT de CARTERAS!")

write_csv(
  x = tab_A, 
  file = "0_data/2_final/cr/carteras_output.csv", 
  na = "" 
)

################################################################################
################################################################################

## Recursos

cli::cli_alert_success("¡Iniciando con RECURSOS!")

rec = cr %>% 
  filter(
    nombre_entidad == "Total",
    tipo == "Recursos"
  ) %>% 
  dplyr::select(-nombre_entidad) 

B = rec %>% 
  mutate(
    # Miles de millones
    valor = valor/1000 
  ) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "valor"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    cdt_recursos,
    vista_recursos,
    total_recursos
  )

B_gr = rec %>%
  filter(year(fecha) > 2002) %>% 
  arrange(macrocuenta, tipo, fecha) %>% 
  group_by(macrocuenta, tipo) %>% 
  mutate(
    gr = (valor - lag(valor, 12)) / lag(valor, 12) * 100,
  ) %>% 
  dplyr::select(-valor)

B_gr_w = B_gr %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "gr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    cdt_recursos,
    vista_recursos,
    total_recursos
  ) %>% 
  rename_with(~ paste0("gr_", .), .cols = -fecha)

B_gr_real = B_gr %>% 
  left_join(inflacion, by = join_by(fecha)) %>% 
  mutate(
    rgr = ((1 + gr/100)/(1 + inflacion/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(gr, inflacion)) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "rgr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    cdt_recursos,
    vista_recursos,
    total_recursos
  ) %>% 
  rename_with(~ paste0("rgr_", .), .cols = -fecha)

B_share = rec %>% 
  filter(
    !grepl("Total", macrocuenta)
  ) %>% 
  rename(valor_sub = valor) %>% 
  left_join(
    rec %>%
      filter(macrocuenta == "Total") %>%
      dplyr::select(-macrocuenta),
    by = c("fecha", "tipo")
  ) %>% 
  mutate(
    share = valor_sub/valor * 100
  ) %>% 
  dplyr::select(-c(valor_sub, valor)) %>% 
  pivot_wider(
    names_from = c("macrocuenta", "tipo"), 
    values_from = "share"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    cdt_recursos,
    vista_recursos
  ) %>% 
  rename_with(~ paste0("sh_", .), .cols = -fecha)


tab_B = B %>% 
  left_join(B_gr_w,    by = "fecha") %>%
  left_join(B_gr_real, by = "fecha") %>%
  left_join(B_share,   by = "fecha") %>% 
  left_join(
    A_w %>% 
      dplyr::select(fecha, total_bruta),
    by = "fecha"
  ) %>% 
  mutate(
    cre_rec = total_bruta/total_recursos * 100
  ) %>% 
  dplyr::select(-total_bruta) %>% 
  left_join(inflacion, by = "fecha") %>% 
  relocate(inflacion, .after = fecha) %>% 
  mutate(
    fecha = as.Date(fecha)
  ) %>% 
  relocate(inflacion, .after = fecha) %>% 
  mutate(across(where(is.numeric), ~ ifelse(
    is.na(.x), NA_character_,
    format(round(.x, 4), nsmall = 4, scientific = FALSE)
  )))

cli::cli_alert_success("¡Exportando el OUTPUT de RECURSOS!")

write_csv(
  x = tab_B, 
  file = "0_data/2_final/cr/recursos_output.csv", 
  na = "" 
)

################################################################################
################################################################################

## TASAS Y DESEMBOLSOS

cli::cli_alert_success("¡Iniciando con TASAS Y DESEMBOLSOS!")

# Dejando solo inflación
gdata::keep(inflacion, sure = T)

# Cargando la data de td
load(file = "0_data/1_processed/mas_reciente/td/td_mas_reciente.RData")

# POR MODALIDAD
A = td_mod_agregada %>% 
  dplyr::select(-part) %>% 
  mutate(
    # Miles de millones
    desembolso_total = desembolso_total/1e6,
    # Miles
    n_creditos = if_else(fecha >= as.Date("2011-01-01"),
                         n_creditos / 1e3,
                         NA_real_)
  ) %>% 
  pivot_longer(
    cols = desembolso_total:tasa_ponderada, 
    names_to = "variable", 
    values_to = "valor"
  )


A_w = A %>% 
  pivot_wider(
    names_from = c("modalidad", "variable"), 
    values_from = "valor"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_desembolso_total, 
    consumo_desembolso_total,
    hipotecaria_desembolso_total,
    credito_productivo_desembolso_total,
    sobregiros_desembolso_total, 
    tarjeta_de_credito_empresarial_desembolso_total,
    total_desembolso_total,
    comercial_tasa_ponderada, 
    consumo_tasa_ponderada,
    hipotecaria_tasa_ponderada,
    credito_productivo_tasa_ponderada,
    sobregiros_tasa_ponderada,
    tarjeta_de_credito_empresarial_tasa_ponderada,
    total_tasa_ponderada,
    comercial_n_creditos,
    consumo_n_creditos,
    hipotecaria_n_creditos,
    credito_productivo_n_creditos,
    sobregiros_n_creditos,
    tarjeta_de_credito_empresarial_n_creditos,
    total_n_creditos
  )

A_gr = A %>%
  arrange(modalidad, variable, fecha) %>% 
  group_by(modalidad, variable) %>% 
  mutate(
    valor_mm3 = zoo::rollmean(valor, k = 3, align = "right", fill = NA),
    gr = if_else(
      condition = variable != "tasa_ponderada", 
      true = (valor_mm3 - lag(valor_mm3, 12)) / lag(valor_mm3, 12) * 100, 
      false = valor - lag(valor, 12)
    )
  ) %>% 
  filter(fecha >= as.Date("2003-07-01")) %>% 
  dplyr::select(-c(valor, valor_mm3))

A_gr_w = A_gr %>% 
  pivot_wider(
    names_from = c("modalidad", "variable"), 
    values_from = "gr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_desembolso_total, 
    consumo_desembolso_total,
    hipotecaria_desembolso_total,
    credito_productivo_desembolso_total,
    sobregiros_desembolso_total, 
    tarjeta_de_credito_empresarial_desembolso_total,
    total_desembolso_total,
    comercial_tasa_ponderada, 
    consumo_tasa_ponderada,
    hipotecaria_tasa_ponderada,
    credito_productivo_tasa_ponderada,
    sobregiros_tasa_ponderada,
    tarjeta_de_credito_empresarial_tasa_ponderada,
    total_tasa_ponderada,
    comercial_n_creditos,
    consumo_n_creditos,
    hipotecaria_n_creditos,
    credito_productivo_n_creditos,
    sobregiros_n_creditos,
    tarjeta_de_credito_empresarial_n_creditos,
    total_n_creditos
  ) %>% 
  rename_with(~ paste0("gr_", .), .cols = -fecha)

A_gr_real = A_gr %>% 
  filter(variable == "desembolso_total") %>% 
  left_join(
    y = inflacion %>% 
      mutate(
        inflacion_3mm = zoo::rollmean(inflacion, k = 3, align = "right", fill = NA),
      ), 
    by = join_by(fecha)
  ) %>% 
  mutate(
    rgr = ((1 + gr/100)/(1 + inflacion_3mm/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(gr, inflacion, inflacion_3mm)) %>% 
  pivot_wider(
    names_from = c("modalidad", "variable"), 
    values_from = "rgr"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_desembolso_total, 
    consumo_desembolso_total,
    hipotecaria_desembolso_total,
    credito_productivo_desembolso_total,
    sobregiros_desembolso_total, 
    tarjeta_de_credito_empresarial_desembolso_total,
    total_desembolso_total
  ) %>% 
  rename_with(~ paste0("rgr_", .), .cols = -fecha)

A_tasa_real = A %>% 
  filter(variable == "tasa_ponderada") %>% 
  left_join(inflacion, by = join_by(fecha)) %>% 
  mutate(
    r = ((1 + valor/100)/(1 + inflacion/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(valor, inflacion)) %>% 
  pivot_wider(
    names_from = c("modalidad", "variable"), 
    values_from = "r"
  ) %>% 
  janitor::clean_names() %>% 
  rename_with(~ paste0("r_", .), .cols = -fecha)

A_share = A %>% 
  filter(
    !grepl("Total", modalidad),
    variable == "desembolso_total"
  ) %>% 
  rename(valor_sub = valor) %>% 
  left_join(
    A %>%
      filter(modalidad == "Total", variable == "desembolso_total") %>%
      dplyr::select(-modalidad),
    by = c("fecha", "variable")
  ) %>% 
  mutate(
    share = valor_sub/valor * 100
  ) %>% 
  dplyr::select(-c(valor_sub, valor)) %>% 
  pivot_wider(
    names_from = c("modalidad", "variable"), 
    values_from = "share"
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    comercial_desembolso_total, 
    consumo_desembolso_total,
    hipotecaria_desembolso_total,
    credito_productivo_desembolso_total,
    sobregiros_desembolso_total, 
    tarjeta_de_credito_empresarial_desembolso_total
  ) %>% 
  rename_with(~ paste0("sh_", .), .cols = -fecha)

## Tabla consolidada
tab_A = A_w %>% 
  left_join(A_gr_w,    by = "fecha") %>%
  left_join(A_gr_real, by = "fecha") %>%
  left_join(A_tasa_real, by = "fecha") %>%
  left_join(A_share,   by = "fecha") %>% 
  left_join(inflacion, by = "fecha") %>% 
  relocate(inflacion, .after = fecha) %>% 
  mutate(
    fecha = as.Date(fecha)
  ) %>% 
  mutate(across(where(is.numeric), ~ ifelse(
    is.na(.x), NA_character_,
    format(round(.x, 4), nsmall = 4, scientific = FALSE)
  )))


cli::cli_alert_success("¡Exportando el OUTPUT de CARTERAS!")

write_csv(
  x = tab_A, 
  file = "0_data/2_final/td/td_output.csv", 
  na = "" 
)
