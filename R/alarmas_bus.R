#' @title Genera 2 posibles tipos de alarma: 1) BUS PARADO EN MEDIO DE UNA RUTA + X MINUTOS y 2) AUSENCIA DE DATOS BUS MIENTRAS ESTE ESTABA EFECTUANDO RUTA
#'
#' @description Genera 2 posibles tipos de alarma: 1) BUS PARADO EN MEDIO DE UNA RUTA + X MINUTOS y 2) AUSENCIA DE DATOS BUS MIENTRAS ESTE ESTABA EFECTUANDO RUTA
#'
#' @param id_dispositivo,tiempo_maximo_parado
#'
#' @return json
#'
#' @examples  tiempos_llegada_paradas("b37c2cc0-0350-11ed-b4eb-0d97eeef399c",7)
#'
#' @import httr
#' jsonlite
#' dplyr
#' sf
#'
#' @export

alarmas_bus <- function(id_dispositivo,tiempo_maximo_parado){

  tiempo_maximo_parado <- as.numeric(tiempo_maximo_parado)
  id_dispositivo <- as.character(id_dispositivo)

  # ------------------------------------------------------------------------------
  # 0) - REFERENCIA PARADAS
  # ------------------------------------------------------------------------------

  ficheros_en_ruta <- list.files(system.file('extdata', package = 'gestionBus'), full.names = TRUE)
  posicion_fichero <- grep("paradas_bus_plasencia",ficheros_en_ruta)
  df_paradas <- read.csv(as.character(ficheros_en_ruta[posicion_fichero]), sep = ",")

  # Linea 1
  df_paradas_linea_1_subida <- df_paradas[df_paradas$linea_1 == 1 & (df_paradas$sentido == 1 | df_paradas$sentido >=2),]
  df_paradas_linea_1_bajada <- df_paradas[df_paradas$linea_1 == 1 & (df_paradas$sentido == 0 | df_paradas$sentido >=2),]

  # Linea 2
  df_paradas_linea_2_subida <- df_paradas[df_paradas$linea_2 == 1 & (df_paradas$sentido == 1 | df_paradas$sentido >=2),]
  df_paradas_linea_2_bajada <- df_paradas[df_paradas$linea_2 == 1 & (df_paradas$sentido == 0 | df_paradas$sentido >=2),]

  # Linea 3
  df_paradas_linea_3_subida <- df_paradas[df_paradas$linea_3 == 1 & (df_paradas$sentido == 1 | df_paradas$sentido >=2),]
  df_paradas_linea_3_bajada <- df_paradas[df_paradas$linea_3 == 1 & (df_paradas$sentido == 0 | df_paradas$sentido >=2),]

  # ------------------------------------------------------------------------------
  # PETICIÓN TOKENs THB
  # ------------------------------------------------------------------------------

  cuerpo <- '{"username":"kepa@techfriendly.es","password":"kepatech"}'
  post <- httr::POST(url = "https://plataforma.plasencia.es/api/auth/login",
                     add_headers("Content-Type"="application/json","Accept"="application/json"),
                     body = cuerpo,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  resultado_peticion_token <- httr::content(post)
  auth_thb <- paste("Bearer",resultado_peticion_token$token)

  # ------------------------------------------------------------------------------
  # 1) - RECEPCIÓN GEOPOSICIONAMIENTO AUTOBÚS
  # ------------------------------------------------------------------------------

  fecha_1 <- Sys.time() - 60*tiempo_maximo_parado # Timestamp actual menos tiempo_maximo_parado mins
  fecha_2 <- Sys.time()

  fecha_1 <- format(as.numeric(as.POSIXct(fecha_1))*1000,scientific = F)
  fecha_2 <- format(as.numeric(as.POSIXct(fecha_2))*1000,scientific = F)

  keys <- URLencode(c("lat,lon,spe"))
  url_thb_fechas <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/timeseries?limit=10000&keys=",keys,"&startTs=",fecha_1,"&endTs=",fecha_2,sep = "")
  peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  # Tratamiento datos. De raw a dataframe
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  print(df)
  df <- df[,-c(3,5)]

  colnames(df) <- c("ts","lat","lon","spe")
  df$spe <- gsub("*. km/h","",df$spe)
  df$spe <- as.numeric(df$spe)
  #df <- df[-grep(" km/h",df$spe),]
  df <- df[df$lat != "none",]
  df$fecha_time <- as.POSIXct(as.numeric(df$ts)/1000, origin = "1970-01-01")

  df_datos_bus <- df
  df_datos_bus$lat <- as.numeric(df_datos_bus$lat)
  df_datos_bus$lon <- as.numeric(df_datos_bus$lon)

  # Si no hay datos, se termina el programa
  if(nrow(df_datos_bus) == 0){
    return(0)
  }


  # ------------------------------------------------------------------------------
  # 2) - GENERACIÓN ALARMA POR BUS PARADO MÁS DE 7 MINUTOS
  # ------------------------------------------------------------------------------
  max_velocidad <- max(df_datos_bus$spe)
  # Si la máxima velocidad en los último tiempo_maximo_parado es <= 10 km/h, alarma por bus parado en medio de ruta.
  if(max_velocidad <= 10){
    alarma_bus_parado <- 1
  }else{
    alarma_bus_parado <- 0
  }

  # Escritura atributos alarma en entidad: dispositivo tipo GPS
  url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")
  json_envio_plataforma <- paste('{"alarma_bus_parado":"', alarma_bus_parado,'"',
                                 '}',sep = "")
  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )



  # ------------------------------------------------------------------------------
  # 2) - GENERACIÓN ALARMA EN CASO DE AUSENCIA DE BUS Y MI ÚLTIMA POSICIÓN NO ES EL GARJE EN LOS ÚLTIMO 20 MINUTOS
  # ------------------------------------------------------------------------------

  # POSICÓN DEL BUS LOS ÚLTIMOS 20 MINUTOS
  fecha_1 <- Sys.time() - 60*10 # Timestamp actual menos 10 mins
  fecha_2 <- Sys.time()

  fecha_1 <- format(as.numeric(as.POSIXct(fecha_1))*1000,scientific = F)
  fecha_2 <- format(as.numeric(as.POSIXct(fecha_2))*1000,scientific = F)

  keys <- URLencode(c("lat,lon,spe"))
  url_thb_fechas <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/timeseries?limit=10000&keys=",keys,"&startTs=",fecha_1,"&endTs=",fecha_2,sep = "")
  peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  # Tratamiento datos. De raw a dataframe
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df <- df[,-c(3,5)]

  colnames(df) <- c("ts","lat","lon","spe")
  df$spe <- gsub("*. km/h","",df$spe)
  df$spe <- as.numeric(df$spe)
  #df <- df[-grep(" km/h",df$spe),]
  df <- df[df$lat != "none",]
  df$fecha_time <- as.POSIXct(as.numeric(df$ts)/1000, origin = "1970-01-01")

  df_datos_bus <- df
  df_datos_bus$lat <- as.numeric(df_datos_bus$lat)
  df_datos_bus$lon <- as.numeric(df_datos_bus$lon)

  # Si no hay datos, se termina el programa
  if(nrow(df_datos_bus) == 0){
    return(0)
  }


  # POSICÓN DEL BUS LOS ÚLTIMOS 20 MINUTOS
  df_paradas_iniciales <- df_paradas[df_paradas$name == "Garage",]  # Filtro parada Garaje

  # Ver si está en geocerca
  lat <- df_paradas_iniciales$latitud
  long <- df_paradas_iniciales$longitud
  # Agrupación de puntos en variable stores
  paradas_sfc <- st_sfc(st_multipoint(cbind(long, lat)), crs = 4326)   # Puntos paradas
  # Cambio a UTM
  paradas_utm <- st_transform(paradas_sfc, "+proj=utm +zone=29")
  # Generación de geocercas en paradas
  paradas_separadas_id <- st_cast(paradas_utm, "POINT")
  geocercas <- st_buffer(paradas_separadas_id, 90)

  df_datos_bus <- df_datos_bus[order(df_datos_bus$ts, decreasing = TRUE),]
  df_datos_bus <- df_datos_bus[1:10,]

  # Bucle para cada uno de los registros de posición capturados del autobus
  ID_GEOCERCA <- c()
  ID_PARADA <- c()
  NOMBRE_PARADA_GEOCERCA <- c()
  for(i in 1:10){ # 10 última posiciones del autobús (primeras 10 del df)

    # Posición bus
    posicion_bus <- st_sfc(st_point(c(df_datos_bus$lon[i], df_datos_bus$lat[i])), crs = 4326)

    # Cambio a UTM
    #paradas_utm <- st_transform(paradas_sfc, "+proj=utm +zone=29")
    posicion_bus_utm     <- st_transform(posicion_bus, "+proj=utm +zone=29")

    # Generación de geocercas en paradas
    #paradas_separadas_id <- st_cast(paradas_utm, "POINT")
    #geocercas <- st_buffer(paradas_separadas_id, 70)

    # Conversión multipunto a punto de la posición del bus
    columnas_utm_posicion_bus <- st_cast(posicion_bus_utm, "POINT")

    # Comprobación si el bus está sobre una geocerca (dataframe booleane de n filas donde n son las paradas de la línea en un sentido, y 1 columna)
    id_posicion_geocerca <- st_contains(geocercas, columnas_utm_posicion_bus, sparse = FALSE)

    # Si el bus está encima de al menos una geocerca:
    if(any(id_posicion_geocerca[,1])){
      id_geocerca_actual <- match(TRUE,id_posicion_geocerca[,1])  # Get id de la geocerca en la que se encuentra el bus

      # GENERACIÓN DF CON GEOCERCAS
      id_parada <- df_paradas_iniciales$id
      nombre_parada <- df_paradas_iniciales$name
      id_geocerca <- 1:length(paradas_separadas_id)
      df_geocercas <- data.frame(id_parada, nombre_parada, id_geocerca, id_posicion_geocerca, geocercas)

      # ID parada donde se encuenta el bus actualmente
      id_parada_deteccion_bus <- df_geocercas$id_parada[df_geocercas$id_geocerca == id_geocerca_actual]
      nombre_parada_deteccion_bus <- df_geocercas$nombre_parada[df_geocercas$id_geocerca == id_geocerca_actual]

      # Volcado en arrays
      ID_GEOCERCA <- c(ID_GEOCERCA, id_geocerca_actual) # Volcado id geocerca en array geocercas
      NOMBRE_PARADA_GEOCERCA <- c(NOMBRE_PARADA_GEOCERCA, nombre_parada_deteccion_bus) # Volcado nombre parada en array geocercas
      ID_PARADA <- c(ID_PARADA, id_parada_deteccion_bus)
    }else{
      # Volcado en arrays
      ID_GEOCERCA <- c(ID_GEOCERCA, NA) # Volcado id geocerca en array geocercas
      NOMBRE_PARADA_GEOCERCA <- c(NOMBRE_PARADA_GEOCERCA, NA) # Volcado nombre parada en array geocercas
      ID_PARADA <- c(ID_PARADA, NA)
    }
  }
  df_datos_bus$ID_GEOCERCA <- ID_GEOCERCA
  df_datos_bus$ID_PARADA <- ID_PARADA
  df_datos_bus$NOMBRE_PARADA_GEOCERCA <- NOMBRE_PARADA_GEOCERCA

  diferencia_tiempo_en_minutos <- as.numeric(difftime(Sys.time(),df$fecha_time[1],"mins"))  # Diferencia entre último registro bus y tiempo actual en minutos
  df_datos_bus_sin_na <- na.omit(df_datos_bus)
  if(nrow(df_datos_bus_sin_na) == 0 & diferencia_tiempo_en_minutos > 10){  # Hace 10 minutos que no envía y su última posición no ha sido el garaje
    alarma_ausencia_datos <- 1
  }else{ # Sí ha estado en el garaje en las últimos 20 minutos
    alarma_ausencia_datos <- 0
  }

  # Escritura atributos alarma en entidad: dispositivo tipo GPS
  url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")
  json_envio_plataforma <- paste('{"alarma_ausencia_datos":"', alarma_ausencia_datos,'"',
                                 '}',sep = "")
  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  return(1)

}
