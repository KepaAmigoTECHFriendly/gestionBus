#' @title Asigna guiones si no hay ningún autobús circulando en la línea
#'
#' @description Asigna guiones si no hay ningún autobús circulando en la línea. Se activa con un cambio de línea
#'
#' @param linea
#'
#' @return json
#'
#' @examples  linea_sin_autobus(3)
#'
#' @import httr
#' jsonlite
#' dplyr
#' lubridate
#'
#' @export

linea_sin_autobus <- function(linea){

  linea <- linea

  #-----------------------------------------------------------------------------
  #-----------------------------------------------------------------------------
  # PARADAS A "-" por horario
  #-----------------------------------------------------------------------------
  #-----------------------------------------------------------------------------
  linea_guion <- 0 # Por defecto 0, a no ser que salte la alarma
  # Función paradas a "-"
  paradas_a_guion <- function(linea_guion){

    linea_guion <- linea_guion

    # 1) GET ACTIVOS
    url_thb_fechas <- "http://plataforma:9090/api/tenant/assets?pageSize=500&page=0"
    peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_activos <- df[df$data.type == "parada",]
    df_activos <- df_activos[order(df_activos$data.name, decreasing = FALSE),]

    df_paradas <- df_paradas[order(df_paradas$name, decreasing = FALSE),]


    # 4) Creación atributos tiempo_llegada_linea_x
    for(i in 1:nrow(df_activos)){
      print(i)

      url <- paste("http://plataforma:9090/api/plugins/telemetry/ASSET/", df_activos$data.id$id[i], "/SERVER_SCOPE",sep = "")

      if(linea_guion == 0 | linea_guion == 1){

        if(df_paradas$linea_1[i] == 1){

          json_envio_plataforma <- paste('{"tiempo_llegada_linea_1":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )

          json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_1":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )
        }
      }

      if(linea_guion == 0 | linea_guion == 2){
        if(df_paradas$linea_2[i] == 1){
          json_envio_plataforma <- paste('{"tiempo_llegada_linea_2":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )

          json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_2":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )
        }
      }

      if(linea_guion == 0 | linea_guion == 3){
        if(df_paradas$linea_3[i] == 1){

          json_envio_plataforma <- paste('{"tiempo_llegada_linea_3":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )

          json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_3":', '"-"',
                                         '}',sep = "")

          post <- httr::POST(url = url,
                             add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                             body = json_envio_plataforma,
                             verify= FALSE,
                             encode = "json",verbose()
          )
        }
      }
    }
  }





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
  # 0.1) - RECOGIDA IDs PARA SABER NÚMERO DE AUTOBUSES QUE CIRCULAN POR LÍNEA Y CALCULAR PRODUCTO DE TIEMPO
  # ------------------------------------------------------------------------------
  # Get dispositivos plataforma
  url_thb <- "http://plataforma:9090/api/tenant/devices?pageSize=10000&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_dispositivos_gps <- df[df$data.type == "GPS",] # Filtrado por GPS

  ids_gps <- df_dispositivos_gps$data.id$id

  linea_vector <- c()
  keys <- URLencode(c("active, Línea"))
  for(i in 1:length(ids_gps)){

    url_gps <- paste("http://plataforma:9090/api/plugins/telemetry/DEVICE/",ids_gps[i],"/values/attributes?",keys,sep = "")
    peticion <- GET(url_gps, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    print(i)
    activo_atributo <- df$value[grep("active",df$key)]
    if(activo_atributo == "FALSE"){next}
    linea_atributo <- df$value[grep("Línea",df$key)]
    linea_vector <- c(linea_vector, linea_atributo)
  }

  linea_vector <- as.numeric(linea_vector)  # Vector de líneas que están circulando actualmente
  lineas_referencia <- c(1,2,3)
  lineas_sin_bus <- setdiff(lineas_referencia,linea_vector)

  # Llamada paradas a guión
  if(length(lineas_sin_bus) == 1){
    paradas_a_guion(lineas_sin_bus)
  }else{
    for(i in 1:length(lineas_sin_bus)){
      paradas_a_guion(lineas_sin_bus[i])
    }
  }
  return(3)
}
