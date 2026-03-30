################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Julio 2025
################################################################################

# Histórico Distribución de cartera
# Consolida la distribución de las carteras

# Nota: Este archivo no debe cambiar, se consolida una sola vez y funciona como
# ancla para ir incluyendo las actualizaciones

################################################################################
# Librerias
{
  library(tidyverse)
}

rm(list = ls())

################################################################################

# Archivos principales y auxiliares

## Cargando la información histórica
dc = readr::read_csv(
  file = "0_data/0_raw/dc_historico/Distribuci_n_de_cartera_por_producto_20250721.csv"
)

## Diccionario de los tipos de subproducto
diccionario = readxl::read_excel(path = "0_data/0_raw/diccionarios/diccionario_dc.xlsx") %>% 
  dplyr::select(1, 4:6) %>% 
  filter(!is.na(modalidad))

## Listado de entidades
entidades = readxl::read_excel(path = "0_data/0_raw/diccionarios/entidades_dc.xlsx") %>% 
  dplyr::select(1, 2, 4)

################################################################################

## Transformaciones iniciales
dc_filtered = dc %>% 
  # Limpiando nombre de las columnas
  janitor::clean_names() %>% 
  # Filtrando por tipo de entidad
  filter(
  # 1 Bancos, 2 Corporaciones financieras, 22 IOEs, 32 Cooperativas caracter financiero, 4 Compañias de financiamiento
    tipo_entidad %in% c("1", "2", "4", "32")
  ) %>%
  mutate(
    # Creando fecha mensual (datos son semanales)
    fecha = floor_date(dmy(fecha_corte), "month")
  ) %>% 
  # Agregando el diccionario de subproductos
  left_join(diccionario, by = join_by(unicap, desc_renglon)) %>% 
  filter(!is.na(modalidad)) %>% 
  # Agregando ID de entidades
  left_join(entidades, by = join_by(tipo_entidad, codigo_entidad)) %>% 
  # Dejando sólo tipos de interés
  dplyr::select(
    fecha, modalidad, subproducto, name, 9, 10
  ) %>% 
  # Renombrando
  setNames(., c(
    "fecha", "modalidad", "subproducto", "nombre_entidad",
    "Bruta", "Vigente" 
    # "Vencida_1_2", "Vencida_2_3",
    # "Vencida_1_3", "Vencida_3_4", "Vencida_4", "Vencida_3_6",
    # "Vencida_6", "Vencida_1_4", "Vencida_4_6", "Vencida_6_12",
    # "Vencida_12_18", "Vencida_12", "Vencida_18",
    # "Riesgo_A", "Riesgo_B", "Riesgo_C", "Riesgo_D", "Riesgo_E"
  )) %>% 
  # Calculando la cartera Vencida
  mutate(
    Vencida = Bruta - Vigente
  ) %>% 
  dplyr::select(-Vigente) %>% 
  # Eliminando duplicados
  dplyr::distinct()

# Llevando la data a formato long
dc_long = dc_filtered %>%
  pivot_longer(
    cols = -c(1:4), 
    names_to = "tipo", values_to = "valor"
  ) %>% 
  mutate(
    # Miles de millones
    valor = valor/1e9 
  )

# Cartera total
dc_total_total = dc_long %>% 
  reframe(
    valor = sum(valor), 
    .by = c("fecha", "tipo")
  ) %>% 
  pivot_wider(
    names_from = tipo, values_from = valor
  ) %>% 
  rename_with(~ paste0(.x, "_total"), -fecha) %>% 
  janitor::clean_names()

# Cartera total por modalidad
dc_modalidad_total = dc_long %>% 
  group_by(fecha, tipo, modalidad) %>% 
  reframe(
    valor = sum(valor)
  ) %>% ungroup() %>% 
  pivot_wider(
    names_from = c(tipo, modalidad), values_from = valor
  ) %>% 
  janitor::clean_names()

# Cartera total por subproductos
dc_subproducto_total = dc_long %>% 
  group_by(fecha, tipo, modalidad, subproducto) %>% 
  reframe(
    valor = sum(valor)
  ) %>% ungroup() %>% 
  mutate(
    nombre_entidad = "Total" 
  )

# Calculando agregado por subproducto y entidades
dc_subproducto = dc_long %>% 
  group_by(fecha, nombre_entidad, tipo, modalidad, subproducto) %>% 
  reframe(
    valor = sum(valor)
  ) %>% 
  ungroup() %>% 
  # Agregando Total
  bind_rows(dc_subproducto_total)

################################################################################

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
