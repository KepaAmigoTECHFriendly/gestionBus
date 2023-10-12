#' @title Cálculo del tiempo de recorrido de línea y sentido
#'
#' @description Cálculo del tiempo de recorrido de línea y sentido
#'
#' @param id_dispositivo,linea,sentido
#'
#' @return json
#'
#' @examples  calculo_tiempo_linea("b37c2cc0-0350-11ed-b4eb-0d97eeef399c",3,1)
#'
#' @import httr
#' jsonlite
#' dplyr
#' sf
#' lubridate
#'
#' @export

calculo_tiempo_linea <- function(id_dispositivo, linea, sentido){

  linea <- linea
  sentido <- as.numeric(sentido)
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
  post <- httr::POST(url = "http://plataforma:9090/api/auth/login",
                     add_headers("Content-Type"="application/json","Accept"="application/json"),
                     body = cuerpo,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  resultado_peticion_token <- httr::content(post)
  auth_thb <- paste("Bearer",resultado_peticion_token$token)




  # ------------------------------------------------------------------------------
  # RESETEO ATRIBUTO aforo_real
  # ------------------------------------------------------------------------------

  json_envio_plataforma <- paste('{"aforo_real":"', 0,'"',
                                 '}',sep = "")
  url <- paste("http://plataforma:9090/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")
  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )



  # ------------------------------------------------------------------------------
  # 1) - RECEPCIÓN GEOPOSICIONAMIENTO AUTOBÚS
  # ------------------------------------------------------------------------------

  fecha_1 <- Sys.time() - 60*40 # Timestamp actual menos 40 mins
  fecha_2 <- Sys.time()

  fecha_1 <- format(as.numeric(as.POSIXct(fecha_1))*1000,scientific = F)
  fecha_2 <- format(as.numeric(as.POSIXct(fecha_2))*1000,scientific = F)

  keys <- URLencode(c("lat,lon,spe"))
  url_thb_fechas <- paste("http://plataforma:9090/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/timeseries?limit=10000&keys=",keys,"&startTs=",fecha_1,"&endTs=",fecha_2,sep = "")
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


  #------------------------------------------------------------------------------
  # 2) - OBTENCIÓN DEL SENTIDO DEL AUTOBÚS
  #------------------------------------------------------------------------------

  # Creación de geocercas
  # Generación de dataframe con paradas en base a la línea en la que está circulando el bus
  posicion_columna_linea_objetivo <- grep(paste("linea_",linea,sep = ""), colnames(df_paradas))
  df_trabajo_paradas_linea_objetivo <- df_paradas[df_paradas[,posicion_columna_linea_objetivo] == 1,]

  # DETECCIÓN SENTIDO (SUBIDA = 1 O BAJADA = 0)
  df_datos_bus <- df_datos_bus[order(df_datos_bus$ts, decreasing = FALSE),]  # Ordenación por marca de tiempo

  # 1 - Detectar si el bus ha partido de una de las paradas de inicio de la ruta
  df_paradas_iniciales <- df_trabajo_paradas_linea_objetivo[df_trabajo_paradas_linea_objetivo$sentido == 2,]  # Get paradas iniciales de la línea objetivo


  lat <- df_paradas_iniciales$latitud
  long <- df_paradas_iniciales$longitud
  # Agrupación de puntos en variable stores
  paradas_sfc <- st_sfc(st_multipoint(cbind(long, lat)), crs = 4326)   # Puntos paradas
  # Cambio a UTM
  paradas_utm <- st_transform(paradas_sfc, "+proj=utm +zone=29")
  # Generación de geocercas en paradas
  paradas_separadas_id <- st_cast(paradas_utm, "POINT")
  geocercas <- st_buffer(paradas_separadas_id, 70)

  df_datos_bus <- df_datos_bus[order(df_datos_bus$ts, decreasing = TRUE),]  # Orden datos bus decreciente por marca temporal


  # Bucle para cada uno de los registros de posición capturados del autobus
  ID_GEOCERCA <- c()
  ID_PARADA <- c()
  NOMBRE_PARADA_GEOCERCA <- c()
  for(i in 1:nrow(df_datos_bus)){

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



  df_datos_bus_sin_na <- na.omit(df_datos_bus)
  df_datos_sin_paradas_duplicadas <- df_datos_bus_sin_na[!duplicated(df_datos_bus_sin_na$NOMBRE_PARADA_GEOCERCA), ]
  df_datos_sin_paradas_duplicadas <- df_datos_sin_paradas_duplicadas[order(df_datos_sin_paradas_duplicadas$ts, decreasing = TRUE),]  # Orden por ts

  if(nrow(df_datos_sin_paradas_duplicadas) == 2){
    tiempo_linea <- round(as.numeric(difftime(as.POSIXct(df_datos_sin_paradas_duplicadas$ts[1]/1000, origin="1970-01-01"), as.POSIXct(df_datos_sin_paradas_duplicadas$ts[2]/1000, origin="1970-01-01"))))
  }else{
    # Finalizo el programa
    print("Se han detectado paso por más de 2 paradas cabecera en menos de 40 minutos")
    return(0)
  }



  #------------------------------------------------------------------------------
  # 3) - POST EN PLATAFORMA
  #-----------------------------------------------------------------------------

  clave <- paste("tiempo_linea_",linea,"_sentido_",sentido,sep = "")

  id_lineas <- "07c323a0-43ee-11ed-b077-bb6dc81b6e02"

  url <- paste("http://plataforma:9090/api/plugins/telemetry/ASSET/", id_lineas, "/timeseries/ANY",sep = "")
  json_envio_plataforma <- paste('{"',clave,'":"', tiempo_linea,'"',
                                 '}',sep = "")

  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  return(1)
}
