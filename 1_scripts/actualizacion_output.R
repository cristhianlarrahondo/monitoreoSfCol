#  ╔═══════════════════════════════════════════════════════════════════════════╗
#   BBVA Research
#   Autor: Cristhian Larrahondo
#   Fecha: Agosto 2025                                                      
# ╚════════════════════════════════════════════════════════════════════════════╝

# == 0. Preparando ambiente ====================================================
rm(list = ls())

# == 1. Cargue de datos ========================================================

## ---- 1.1 Datos ya procesados ----------------------------------------
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

rm(list = ls())
