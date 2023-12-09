#' @title Asigna línea fuera de servicio cuando el autobús deja de emitir.
#'
#' @description Asigna línea fuera de servicio cuando el autobús deja de emitir. El trigger es el eventro "active == FALSE" generado en plataforma
#'
#' @param id_dispositivo
#'
#' @return json
#'
#' @examples  autobus_fuera_de_servicio("b37c2cc0-0350-11ed-b4eb-0d97eeef399c")
#'
#' @import httr
#' jsonlite
#' dplyr
#' lubridate
#'
#' @export

autobus_fuera_de_servicio <- function(id_dispositivo){

  id_dispositivo <- as.character(id_dispositivo)

  # ------------------------------------------------------------------------------
  # PETICIÓN TOKENs THB
  # ------------------------------------------------------------------------------

  cuerpo <- fromJSON("/opt/extra_data/config_cred.json")
  cuerpo <- paste('{"username":"',cuerpo$username,'","password":"',cuerpo$password,'"}',sep = "")
  post <- httr::POST(url = "http://plataforma:9090/api/auth/login",
                     add_headers("Content-Type"="application/json","Accept"="application/json"),
                     body = cuerpo,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  resultado_peticion_token <- httr::content(post)
  auth_thb <- paste("Bearer",resultado_peticion_token$token)


  # ------------------------------------------------------------------------------
  # ESCRITURA ATRIBUTO "Fuera de servicio"
  # ------------------------------------------------------------------------------

  url <- paste("http://plataforma:9090/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")

  json_envio_plataforma <- '{"Línea":"Fuera de servicio"}'

  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  return(3)

}
