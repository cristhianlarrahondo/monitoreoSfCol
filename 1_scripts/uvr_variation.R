################################################################################
# BBVA Research
# Autor: Cristhian Larrahondo
# Fecha: Abril 2025
################################################################################

# Carga y transforma el archivo de UVR

## Usado para ajustar las tasas de interés de créditos con colocación en UVR
## que a partir del 29 de septiembre de 2023 no incluyen la variación de la UVR

################################################################################

# Elementos antes de ejecutar
initial_vars = ls()

# URL de UVR
uvr_url = "https://suameca.banrep.gov.co/estadisticas-economicas-back/reporte-oac.html?path=%2FEstadisticas_Banco_de_la_Republica%2F1_Precios_e_Inflacion%2F6_Unidad_de_Valor_Real%2F1_Unidad_de_Valor_Real_UVR"

# Mostrar mensaje llamativo
cli::cli_alert_info(
  text = paste0(
    crayon::make_style("orange", bg = FALSE)(cli::style_bold("No olvide primero actualizar el archivo de UVR aquí:\n")),
    "<", uvr_url, ">\n\n",
    "¿Actualizó el archivo?\n",
    "Si es así, escriba ", crayon::green$bold("y"), ", de lo contrario ", crayon::red$bold("n")
  )
)

# Leer respuesta del usuario
respuesta = tolower(readline(prompt = crayon::yellow$bold("Ingrese su respuesta (y/n): ")))

# Verificar la respuesta
if (respuesta != "y") {
  cli::cli_alert_danger("El proceso se detuvo porque no se actualizó el archivo UVR.")
  stop("Actualice el archivo y vuelva a correr esta línea")
  
  final_vars = ls()
  new_vars = setdiff(final_vars, initial_vars)
  rm(list = new_vars)
  
} else {
  cli::cli_alert_success("¡Gracias! Continuando con el proceso...")
  
  path = "0_data/0_raw/uvr/"
  
  filename = list.files(path = path, pattern = ".xlsx")
  
  uvr = readxl::read_excel(
    path = glue::glue("{path}{filename}"), 
    col_types = c("date", "numeric", "numeric")
  )
  
  uvr_var = uvr %>% 
    dplyr::select(1, 3) %>% 
    setNames(., c("fecha", "uvr_var")) %>% 
    mutate(
      fecha = floor_date(fecha, unit = "month")
    ) %>% 
    group_by(fecha) %>% 
    reframe(
      uvr_var = mean(uvr_var, na.rm = T)
    )
  
  saveRDS(
    object = uvr_var, 
    file = "0_data/1_processed/uvr_variation.rds"
  )
  
  x = cli::rule(center = "🎉 Datos de variación UVR cargados", line = "=", col = "green")
  print(x)
  
  final_vars = ls()
  new_vars = setdiff(final_vars, initial_vars)
  rm(list = new_vars)
  
}

