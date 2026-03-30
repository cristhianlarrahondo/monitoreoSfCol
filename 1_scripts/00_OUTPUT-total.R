################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Julio 2025
################################################################################

# Output: Cartera y depósitos

################################################################################
# Librerias
{
  library(tidyverse)
}

rm(list = ls())

################################################################################
# Datos generales
################################################################################

# Inflacion total
# https://suameca.banrep.gov.co/estadisticas-economicas-back/reporte-oac.html?path=%2FEstadisticas_Banco_de_la_Republica%2F1_Precios_e_Inflacion%2F1_Inflacion_y_meta%2F1_Inflacion_y_meta

# Archivo de inflación
file_inflacion = list.files(
  path = "0_data/0_raw/inflacion/", pattern = ".xlsx", full.names = T
)

# Cargando el archivo
inflacion = readxl::read_excel(
  path = glue::glue("{file_inflacion}"), 
  skip = 1,
  col_names = F
) %>% 
  dplyr::select(1, 2) %>% 
  setNames(., c("fecha", "inflacion")) %>% 
  mutate(fecha = as.Date(floor_date(fecha, unit = "month")))

################################################################################

# CRÉDITO

################################################################################

# Titularizaciones

file_titularizaciones = list.files(
  path = "0_data/0_raw/titularizaciones/", pattern = ".xlsx", full.names = T
)

# Titularizaciones por tipo
titu_tipo = readxl::read_excel(
  path = glue::glue("{file_titularizaciones}"), 
  sheet = "Tipo de Crédito Hipotecario", 
  col_names = T,
  skip = 7
) %>% 
  setNames(., c("fecha", "VIS", "NoVIS", "Total")) %>% 
  filter(!is.na(VIS)) %>% 
  mutate(
    fecha = as.Date(as.numeric(fecha), origin = "1899-12-30"),
    fecha = floor_date(fecha, unit = "month")
  )

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

################################################################################ 

### Carteras y depósitos (Millones)
cr = readRDS(file = "0_data/1_processed/mas_reciente/cr/cr_mas_reciente.rds") %>% 
  arrange(nombre_entidad, tipo, macrocuenta, fecha)

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

###

# Leasing
source("1_scripts/output_leasing.R") # Hay un error de rm que corregir
leasing_data = readRDS(file = "0_data/0_raw/pulled/leasing/leasing_mas_reciente.rds")

leasing = leasing_data %>% 
  filter(
    moneda == "0",
    # Sólo Bancos y CFs
    tipo_entidad %in% c("1", "4")
  ) %>%
  mutate(
    valor = as.numeric(valor),
    fecha = floor_date(ymd_hms(fecha_corte), "month"),
    macrocuenta = ifelse(tipo_entidad == "1", "Leasing_Bancos", "Leasing_CF")
  ) %>% 
  group_by(fecha, macrocuenta) %>% 
  reframe(
    # En millones
    valor = sum(valor, na.rm = T)/1e6
  ) %>% ungroup() %>% 
  mutate(
    tipo = "Bruta"
  )

################################################################################

# OUTPUT: tabla de carteras

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

################################################################################

# RECURSOS

################################################################################


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

write_csv(
  x = tab_A, 
  file = "0_data/2_final/cr/carteras_output.csv", 
  na = "" 
)

write_csv(
  x = tab_B, 
  file = "0_data/2_final/cr/recursos_output.csv", 
  na = "" 
)

################################################################################

# DISTRIBUCIÓN CARTERA

################################################################################

# Dejando solo inflación
gdata::keep(inflacion, sure = T)

# Cargando la data de td
load(file = "0_data/1_processed/mas_reciente/dc/dc_mas_reciente.RData")

##
modalidad = c("Comercial", "Consumo", "Hipotecaria", "Microcrédito")
comercial = c(
  "Corporativo", "Leasing", "Oficial o Gobierno", "Pymes", "Construcción",
  "Moneda extranjera", "Empresarial", "Microempresa", "Finan. e institucional", "Factoring"
) %>% janitor::make_clean_names()


# Saldos wide-format
A = dc_subproducto %>% 
  filter(
    nombre_entidad == "Total",
    modalidad == modalidad[1]
  ) %>% 
  dplyr::select(-c(nombre_entidad, modalidad))

# Full selection vector
cols_to_select = c(
  "bruta_total",
  paste0("bruta_", tolower(modalidad[1])),
  paste0("bruta_", comercial), 
  "vencida_total",
  paste0("vencida_", tolower(modalidad[1])),
  paste0("vencida_", comercial)
)

# Wide-format
A_w = A %>% 
  pivot_wider(
    names_from = c("tipo", "subproducto"),
    values_from = valor
  ) %>% 
  janitor::clean_names() %>% 
  left_join(
    dc_total_total, by = join_by(fecha)
  ) %>% 
  left_join(dc_modalidad_total, by = join_by(fecha)) %>% 
  select(
    fecha,
    all_of(cols_to_select)
  )

A_l = A_w %>% 
  pivot_longer(
    cols = -c(fecha), names_to = "variable", values_to = "valor"
  ) %>% 
  arrange(variable, fecha) #%>% 

# Crecimiento nominal

A_lgr = A_l %>% 
  group_by(variable) %>% 
  mutate(
    gr = (valor - lag(valor, 12)) / lag(valor, 12) * 100,
  ) %>% 
  dplyr::select(-valor) %>% 
  filter(!is.na(gr))

A_gr_w = A_lgr %>% 
  pivot_wider(
    names_from = variable, values_from = gr
  ) %>% 
  dplyr::select(
    fecha,
    all_of(cols_to_select)
  ) %>% 
  rename_with(~ paste0("gr_", .), .cols = -fecha)

# Crecimiento real
A_rgr = A_lgr %>% 
  left_join(inflacion, by = join_by(fecha)) %>% 
  mutate(
    rgr = ((1 + gr/100)/(1 + inflacion/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(inflacion, gr)) %>% 
  pivot_wider(
    names_from = variable, values_from = rgr
  ) %>% 
  dplyr::select(
    fecha,
    all_of(cols_to_select)
  ) %>% 
  rename_with(~ paste0("rgr_", .), .cols = -fecha)

# NPL
A_npl = A %>% 
  pivot_wider(
    names_from = tipo, values_from = valor
  ) %>% 
  mutate(
    npl = Vencida/Bruta * 100
  ) %>% 
  dplyr::select(fecha, subproducto, npl) %>% 
  pivot_wider(
    names_from = subproducto, values_from = npl
  ) %>% 
  janitor::clean_names() %>% 
  dplyr::select(
    fecha,
    all_of(comercial)
  ) %>% 
  rename_with(~ paste0("npl_", .), .cols = -fecha)

tab = A_w %>% 
  left_join(A_gr_w,    by = "fecha") %>%
  left_join(A_rgr, by = "fecha") %>%
  left_join(A_npl, by = "fecha") %>% 
  left_join(inflacion, by = "fecha") %>% 
  relocate(inflacion, .after = fecha) %>% 
  mutate(
    fecha = as.Date(fecha)
  ) %>% 
  mutate(across(where(is.numeric), ~ ifelse(
    is.na(.x), NA_character_,
    format(round(.x, 4), nsmall = 4, scientific = FALSE)
  )))

write_csv(
  x = tab, 
  file = "0_data/2_final/dc/dc_output.csv", 
  na = "" 
)


################################################################################

modalidad = c("Comercial", "Consumo", "Hipotecaria", "Microcrédito")

subproducto = list(
  comercial = c(
    "Corporativo", "Leasing", "Oficial o Gobierno", "Pymes", "Construcción",
    "Moneda extranjera", "Empresarial", "Microempresa", "Finan. e institucional", "Factoring"
  ) %>% janitor::make_clean_names(),
  
  consumo = c(
    "Libranza", "Libre inversión", "Tarjetas de crédito", "Vehículo", 
    "Crédito rotativo", "Otros", "Empleados", "Bajo monto"
  ) %>% janitor::make_clean_names(),
  
  hipotecaria = c(
    "Vivienda No VIS", "Vivienda VIS", "Leasing No VIS", "Leasing VIS",
    "Empleados No VIS", "Empleados VIS", "Libranza No VIS", "Libranza VIS"
  ) %>% janitor::make_clean_names(),
  
  microcredito = c(
    "SMML 25", "SMML 25 y 120"
  ) %>% janitor::make_clean_names()
)


################################################################################

for (m in seq_along(modalidad)) {
  
  #m = 4
  mod = modalidad[m]
  mod_clean = janitor::make_clean_names(mod)
  
  print(glue::glue("Empezando con la cartera: {mod}"))
  
  subp = subproducto[[m]]   
  
  # Saldos wide-format
  A = dc_subproducto %>% 
    filter(
      nombre_entidad == "Total",
      modalidad == mod
    ) %>% 
    dplyr::select(-c(nombre_entidad, modalidad))
  
  # Full selection vector
  cols_to_select = c(
    "bruta_total",
    paste0("bruta_", mod_clean),
    paste0("bruta_", subp), 
    "vencida_total",
    paste0("vencida_", mod_clean),
    paste0("vencida_", subp)
  )
  
  # Wide-format
  A_w = A %>% 
    pivot_wider(
      names_from = c("tipo", "subproducto"),
      values_from = valor
    ) %>% 
    janitor::clean_names() %>% 
    left_join(
      dc_total_total, by = join_by(fecha)
    ) %>% 
    left_join(dc_modalidad_total, by = join_by(fecha)) %>% 
    select(
      fecha,
      all_of(cols_to_select)
    )
  
  A_l = A_w %>% 
    pivot_longer(
      cols = -c(fecha), names_to = "variable", values_to = "valor"
    ) %>% 
    arrange(variable, fecha) #%>% 
  
  # Crecimiento nominal
  
  A_lgr = A_l %>% 
    group_by(variable) %>% 
    mutate(
      gr = (valor - lag(valor, 12)) / lag(valor, 12) * 100,
    ) %>% 
    dplyr::select(-valor) %>% 
    filter(!is.na(gr))
  
  A_gr_w = A_lgr %>% 
    pivot_wider(
      names_from = variable, values_from = gr
    ) %>% 
    dplyr::select(
      fecha,
      all_of(cols_to_select)
    ) %>% 
    rename_with(~ paste0("gr_", .), .cols = -fecha)
  
  # Crecimiento real
  A_rgr = A_lgr %>% 
    left_join(inflacion, by = join_by(fecha)) %>% 
    mutate(
      rgr = ((1 + gr/100)/(1 + inflacion/100) - 1) * 100
    ) %>% 
    dplyr::select(-c(inflacion, gr)) %>% 
    pivot_wider(
      names_from = variable, values_from = rgr
    ) %>% 
    dplyr::select(
      fecha,
      all_of(cols_to_select)
    ) %>% 
    rename_with(~ paste0("rgr_", .), .cols = -fecha)
  
  # NPL
  A_npl = A %>% 
    pivot_wider(
      names_from = tipo, values_from = valor
    ) %>% 
    mutate(
      npl = Vencida/Bruta * 100
    ) %>% 
    dplyr::select(fecha, subproducto, npl) %>% 
    pivot_wider(
      names_from = subproducto, values_from = npl
    ) %>% 
    janitor::clean_names() %>% 
    dplyr::select(
      fecha,
      all_of(subp)
    ) %>% 
    rename_with(~ paste0("npl_", .), .cols = -fecha)
  
  tab = A_w %>% 
    left_join(A_gr_w,    by = "fecha") %>%
    left_join(A_rgr, by = "fecha") %>%
    left_join(A_npl, by = "fecha") %>% 
    left_join(inflacion, by = "fecha") %>% 
    relocate(inflacion, .after = fecha) %>% 
    mutate(
      fecha = as.Date(fecha)
    ) %>% 
    mutate(across(where(is.numeric), ~ ifelse(
      is.na(.x), NA_character_,
      format(round(.x, 4), nsmall = 4, scientific = FALSE)
    )))
  
  write_csv(
    x = tab, 
    file = glue::glue("0_data/2_final/dc/dc_{tolower(mod)}_output.csv"), 
    na = "" 
  )
}

################################################################################

# TASAS Y DESEMBOLSOS

################################################################################

# Dejando solo inflación
gdata::keep(inflacion, sure = T)

# Cargando la data de td
load(file = "0_data/1_processed/mas_reciente/td/td_mas_reciente.RData")

# POR MODALIDAD
A = td_mod_agregada %>% 
  dplyr::select(-part) %>% 
  mutate(
    # Miles de millones
    desembolso_total = desembolso_total/1e3,
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
    #sobregiros_desembolso_total, 
    #tarjeta_de_credito_empresarial_desembolso_total,
    total_desembolso_total,
    comercial_tasa_ponderada, 
    consumo_tasa_ponderada,
    hipotecaria_tasa_ponderada,
    credito_productivo_tasa_ponderada,
    #sobregiros_tasa_ponderada,
    #tarjeta_de_credito_empresarial_tasa_ponderada,
    total_tasa_ponderada,
    comercial_n_creditos,
    consumo_n_creditos,
    hipotecaria_n_creditos,
    credito_productivo_n_creditos,
    #sobregiros_n_creditos,
    #tarjeta_de_credito_empresarial_n_creditos,
    total_n_creditos
  )

A_gr = A %>%
  arrange(modalidad, variable, fecha) %>% 
  group_by(modalidad, variable) %>% 
  mutate(
    gr = if_else(
      condition = variable != "tasa_ponderada", 
      true = (valor - lag(valor, 12)) / lag(valor, 12) * 100, 
      false = valor - lag(valor, 12)
    )
  ) %>% 
  filter(fecha >= as.Date("2003-05-01")) %>% 
  dplyr::select(-valor)

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
    #sobregiros_desembolso_total, 
    #tarjeta_de_credito_empresarial_desembolso_total,
    total_desembolso_total,
    comercial_tasa_ponderada, 
    consumo_tasa_ponderada,
    hipotecaria_tasa_ponderada,
    credito_productivo_tasa_ponderada,
    #sobregiros_tasa_ponderada,
    #tarjeta_de_credito_empresarial_tasa_ponderada,
    total_tasa_ponderada,
    comercial_n_creditos,
    consumo_n_creditos,
    hipotecaria_n_creditos,
    credito_productivo_n_creditos,
    #sobregiros_n_creditos,
    #tarjeta_de_credito_empresarial_n_creditos,
    total_n_creditos
  ) %>% 
  rename_with(~ paste0("gr_", .), .cols = -fecha)

A_gr_real = A_gr %>% 
  filter(variable == "desembolso_total") %>% 
  left_join(inflacion, by = join_by(fecha)) %>% 
  mutate(
    rgr = ((1 + gr/100)/(1 + inflacion/100) - 1) * 100
  ) %>% 
  dplyr::select(-c(gr, inflacion)) %>% 
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
    #sobregiros_desembolso_total, 
    #tarjeta_de_credito_empresarial_desembolso_total,
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
  #sobregiros_desembolso_total, 
  #tarjeta_de_credito_empresarial_desembolso_total
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


write_csv(
  x = tab_A, 
  file = "0_data/2_final/td/td_output.csv", 
  na = "" 
)


################################################################################
