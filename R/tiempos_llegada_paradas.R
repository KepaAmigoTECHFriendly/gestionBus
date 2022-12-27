#' @title Asigna el tiempo de llegada a las marquesinas destino, actualizando el atributos en la plataforma
#'
#' @description Asigna el tiempo de llegada a las marquesinas destino, actualizando el atributos en la plataforma
#'
#' @param id_dispositivo,linea
#'
#' @return json
#'
#' @examples  tiempos_llegada_paradas("b37c2cc0-0350-11ed-b4eb-0d97eeef399c",3)
#'
#' @import httr
#' jsonlite
#' dplyr
#' sf
#' lubridate
#'
#' @export

tiempos_llegada_paradas <- function(id_dispositivo, linea){

  linea_original <- linea
  flag_ultimo_trayecto <- grepl("Último", linea_original)

  hora <- hour(Sys.time())
  hora <- ifelse(hora < 10, paste("0",as.character(hora),sep = ""),hora)
  hora_actual <- paste(as.character(hora),":",as.character(minute(Sys.time())),sep = "")

  # Datos recogidos por plataforma
  id_dispositivo <- as.character(id_dispositivo)
  if(grepl("Último", linea)){
    linea <- as.numeric(gsub(" - Último trayecto","",linea))
  }else if(linea == "Fuera de servicio" & hora_actual < "22:30"){
    return(0)
  }else{
    if(linea == "Fuera de servicio" & hora_actual > "22:30"){
      linea <- 1
    }else{
      linea <- as.numeric(linea)
    }
  }


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




  #-----------------------------------------------------------------------------
  #-----------------------------------------------------------------------------
  # PARADAS A "-" por horario
  #-----------------------------------------------------------------------------
  #-----------------------------------------------------------------------------

  if(hora_actual > "22:30"){
    # ------------------------------------------------------------------------------
    # CREACIÓN ACTIVOS
    # ------------------------------------------------------------------------------

    # 2) GET ACTIVOS
    url_thb_fechas <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
    peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_activos <- df[df$data.type == "parada",]
    df_activos <- df_activos[order(df_activos$data.name, decreasing = FALSE),]

    df_paradas <- df_paradas[order(df_paradas$name, decreasing = FALSE),]



    # 4) Creación atributos tiempo_llegada_linea_x
    for(i in 1:nrow(df_activos)){
      print(i)

      url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", df_activos$data.id$id[i], "/SERVER_SCOPE",sep = "")

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

    return(2)
  }



  # ------------------------------------------------------------------------------
  # 1) - RECEPCIÓN GEOPOSICIONAMIENTO AUTOBÚS
  # ------------------------------------------------------------------------------

  fecha_1 <- Sys.time() - 60*10 # Timestamp actual menos 20 mins
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
  df_datos_bus <- df_datos_bus[1:10,]


  # Bucle para cada uno de los registros de posición capturados del autobus
  ID_GEOCERCA <- c()
  ID_PARADA <- c()
  NOMBRE_PARADA_GEOCERCA <- c()
  for(i in 1:10){ # 10 primeras posiciones del autobús

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


  # SI EL BUS ESTÁ EN UNA DE LAS GEOCERCAS DE INICIO, RESETEAR DATO DE AFORO
  tryCatch({
    # Escribir en API para resetear aforo
    if(nrow(df_datos_sin_paradas_duplicadas) != 0){
      url_api <- "https://encuestas.plasencia.es:2222/bus_stats_reset/"
      # GET NÚMERO BUS
      keys <- URLencode(c("Número"))
      url_thb <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
      peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

      df <- jsonlite::fromJSON(rawToChar(peticion$content))
      df <- as.data.frame(df)
      numero <- df$value

      url_api <- paste("https://encuestas.plasencia.es:2222/bus_stats_reset/",numero,sep = "")
      peticion <- GET(url_api, add_headers("Content-Type"="application/json","Accept"="application/json"), timeout(3))
    }
  },error = function(e){
    print("ERROR POR EXCEPCIÓN AL INTENTAR RESETAR EL DATO DE AFORO")
  })




  # CÁLCULO SENTIDO
  # Cálculo sentido si está en parada de inicio
  if(nrow(df_datos_sin_paradas_duplicadas) != 0){  # El bus se encuentra en una parada de inicio
    id_parada_inicial <- df_datos_sin_paradas_duplicadas$ID_PARADA[1]
    if(linea == 1){
      if(id_parada_inicial == 65 | id_parada_inicial == 59){ # Hogar de Nazaret o Gabriel y Galán 1
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 115 | id_parada_inicial == 15){ # Sociosanitario o Bomberos
        sentido <- 1  # Subiendo
      }
    }else if(linea == 2){
      if(id_parada_inicial == 66){ # Hospital
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 55){ # Estación de tren
        sentido <- 1  # Subiendo
      }
    }else if(linea == 3){
      if(id_parada_inicial == 66){ # Hospital
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 100 | id_parada_inicial == 99){ # PIR Los Monjes 2 o PIR Los Monjes 1_subida
        sentido <- 1  # Subiendo
      }
    }
  }else{ # El bus está en trayecto. Para coger el sentido, es necesario recoger el atributo
    keys <- URLencode(c("parada_destino,sentido"))
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    if(any(grepl("sentido",df$key))){
      sentido <- as.numeric(df$value[grep("sentido",df$key)])
    }else{  # No hay atributo de sentido previamente calculado, por lo que es necesario calcularlo por diferencia de latitudes
      if(nrow(df_datos_sin_paradas_duplicadas) == 0){  # Cojo el sentido solo por la diferencia de longitudes ya que no he encontrado parada de inicio
        # Comprobación de sentido por diferencia de longitudes
        if((df_datos_bus$lat[1] - df_datos_bus$lat[nrow(df_datos_bus)]) > 0) { # Si la resta de la primera y última latitud es negativa, está subiendo
          sentido <- 0
        }else{
          sentido <- 1
        }
      }else{
        id_parada_inicial <- df_datos_sin_paradas_duplicadas$ID_PARADA
        if(id_parada_inicial == 65 | id_parada_inicial == 59 | id_parada_inicial == 66){ # Paradas de salida en sentido bajando
          sentido_parada <- 0  # Bajando
        }else{
          sentido_parada <- 1  # Subiendo
        }

        # Comprobación de sentido por diferencia de longitudes
        if((df_datos_bus$lat[1] - df_datos_bus$lat[nrow(df_datos_bus)]) > 0) { # Si la resta de la primera y última latitud es negativa, está subiendo
          sentido_lat <- 0
        }else{
          sentido_lat <- 1
        }

        sentido <- sentido_lat + sentido_parada
        if(sentido == 0 | sentido == 2){ # Se ha obtenido correctamente el sentido del bus
          if(sentido == 2){
            sentido <- 1
          }
        }else{
          return(0)  # No se puede asegurar el sentido del autobus
        }
      }
    }
  }



  # RECOGIDA DE PARADAS UNA VEZ CONOCIDO EL SENTIDO Y LA LÍNEA DEL BUS
  df_trabajo_paradas <- df_trabajo_paradas_linea_objetivo[df_trabajo_paradas_linea_objetivo$sentido == sentido | df_trabajo_paradas_linea_objetivo$sentido >=2,]

  # POST ACTUALIZACIÓN ATRIBUTO parada_destino UNA VEZ QUE SE CONOCE LA LÍNEA Y EL SENTIDO
  if(linea == 1){
    if(sentido == 0){
      parada_destino <- "Sociosanitario"
      parada_destino_activos_parada <- "Sociosanitario"
    }else{
      parada_destino <- "P. La Data"
      parada_destino_activos_parada <- "P. La Data"
    }
  }else if(linea == 2){
    if(sentido == 0){
      parada_destino <- "Estación de tren"
      parada_destino_activos_parada <- "Estación de tren"
    }else{
      parada_destino <- "Hospital"
      parada_destino_activos_parada <- "Hospital"
    }
  }else if(linea == 3){
    if(sentido == 0){
      parada_destino <- "PIR Los Monjes"
      parada_destino_activos_parada <- "PIR Los Monjes"
    }else{
      parada_destino <- "Hospital"
      parada_destino_activos_parada <- "Hospital"
    }
  }

  # Escritura parada destino en entidad tipo dispositivo GPS
  url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")
  json_envio_plataforma <- paste('{"parada_destino":"', parada_destino,'",', '"sentido":', sentido,
                                 '}',sep = "")
  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )


  # Escritura parada destino en entidad tipo activo Parada
  # GET ACTIVOS TIPO "PARADA"
  url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_activos_parada_escritura_destino <- df[df$data.type == "parada",]

  # Filtrado por nombre marquesinas restantes en línea bus
  nombre_paradas_objetivo <- df_trabajo_paradas$name
  df_activos_parada_escritura_destino <- df_activos_parada_escritura_destino[which(df_activos_parada_escritura_destino$data.name %in% nombre_paradas_objetivo),]

  json_envio_plataforma <- paste('{"parada_destino_linea',linea,'":"', parada_destino_activos_parada,'"',
                                 '}',sep = "")
  for(i in 1:nrow(df_activos_parada_escritura_destino)){
    if(any(grepl(df_activos_parada_escritura_destino$data.name[i],df_paradas_iniciales$name))){
      if(linea == 3){
        switch(df_activos_parada_escritura_destino$data.name[i],
               "Hospital" ={parada_destino_activos_parada <- "PIR Los Monjes"},
               "PIR Los Monjes 2" = {parada_destino_activos_parada <- "Hospital"},
               "Hogar de Nazaret" ={parada_destino_activos_parada <- "Sociosanitario"},
               "Sociosanitario" ={parada_destino_activos_parada <- "P. La Data"},
               "Estación de tren" ={parada_destino_activos_parada <- "Hospital"},
               "Hospital" ={parada_destino_activos_parada <- "PIR Los Monjes"},
        )
      }else{
        switch(df_activos_parada_escritura_destino$data.name[i],
               "Hospital" ={parada_destino_activos_parada <- "PIR Los Monjes"},
               "PIR Los Monjes 2" = {parada_destino_activos_parada <- "Hospital"},
               "Hogar de Nazaret" ={parada_destino_activos_parada <- "Sociosanitario"},
               "Sociosanitario" ={parada_destino_activos_parada <- "P. La Data"},
               "Estación de tren" ={parada_destino_activos_parada <- "Hospital"},
               "Hospital" ={parada_destino_activos_parada <- "Estación de tren"},
        )
      }

    }

    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", df_activos_parada_escritura_destino$data.id$id[i], "/SERVER_SCOPE",sep = "")

    post <- httr::POST(url = url,
                       add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                       body = json_envio_plataforma,
                       verify= FALSE,
                       encode = "json",verbose()
    )
  }


  if(flag_ultimo_trayecto == FALSE){
    # GET HORARIOS ÚLTIMO TRAYECTO POR LÍNEA Y SENTIDO
    # GET ACTIVOS TIPO "Línea BUS"
    url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
    peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_horario_ultimo_trayecto_linea_sentido <- df[df$data.type == "Línea BUS",]
    df_horario_ultimo_trayecto_linea_sentido <- df[df$data.name == paste("Línea ",linea,sep = ""),]

    dia_semana <- wday(Sys.Date()) #Domingo == 1
    if(dia_semana != 1 | dia_semana != 7){
      # Atributo horarios
      if(sentido == 1){
        keys <- URLencode(c("horario_ultimo_trayecto_laborables_subida"))
      }else{
        keys <- URLencode(c("horario_ultimo_trayecto_laborables_bajada"))
      }
    }else{
      # Atributo horarios
      if(sentido == 1){
        keys <- URLencode(c("horario_ultimo_trayecto_festivos_sabado_domingo_subida"))
      }else{
        keys <- URLencode(c("horario_ultimo_trayecto_festivos_sabado_domingo_bajada"))
      }
    }

    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_horario_ultimo_trayecto_linea_sentido$data.id$id,"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_horario_ultimo_trayecto <- df$value
    hora <- hour(Sys.time())
    hora <- ifelse(hora < 10, paste("0",as.character(hora),sep = ""),hora)
    hora_actual <- paste(as.character(hora),":",as.character(minute(Sys.time())),sep = "")
    if(hora_actual > df_horario_ultimo_trayecto){  # Estamos en el último tryeto
      flag_ultimo_trayecto <- TRUE
    }
  }






  #------------------------------------------------------------------------------
  # 3) - ACTUALIZACIÓN TIEMPOS DE LLEGADA A CADA PARADA
  #-----------------------------------------------------------------------------

  # Referencias tiempos
  if(linea == 1){
    if(sentido == 0){
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L1.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L1.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }else if(linea == 2){
    if(sentido == 0){
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L2.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L2.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }else if(linea == 3){
    if(sentido == 0){
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L3.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L3.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }


  # Búsqueda de última geocerca donde ha estado o está el autobús
  lat <- df_trabajo_paradas$latitud
  long <- df_trabajo_paradas$longitud

  # Agrupación de puntos en variable stores
  paradas_sfc <- st_sfc(st_multipoint(cbind(long, lat)), crs = 4326)   # Puntos paradas

  ID_GEOCERCA <- c()
  ID_PARADA <- c()
  NOMBRE_PARADA_GEOCERCA <- c()
  df_datos_bus$ID_GEOCERCA <- replicate(nrow(df_datos_bus), NA)
  df_datos_bus$ID_PARADA <- replicate(nrow(df_datos_bus), NA)
  df_datos_bus$NOMBRE_PARADA_GEOCERCA <- replicate(nrow(df_datos_bus), NA)

  # PARADAS, NO HACE FALTA ITERAR
  # Cambio a UTM
  paradas_utm <- st_transform(paradas_sfc, "+proj=utm +zone=29")
  # Generación de geocercas en paradas
  paradas_separadas_id <- st_cast(paradas_utm, "POINT")
  geocercas <- st_buffer(paradas_separadas_id, 70)

  # BUCLE POR CADA UNO DE LOS REGISTROS DE POSICIÓN DEL AUTOBUS
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
      id_parada <- df_trabajo_paradas$id
      nombre_parada <- df_trabajo_paradas$name
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
  if(nrow(df_datos_bus_sin_na) == 0){return(0)}  # Acaba el programa si el autobus no está en ninguna geocerca

  df_datos_sin_paradas_duplicadas <- df_datos_bus_sin_na[!duplicated(df_datos_bus_sin_na$NOMBRE_PARADA_GEOCERCA), ]
  df_datos_sin_paradas_duplicadas <- df_datos_sin_paradas_duplicadas[order(df_datos_sin_paradas_duplicadas$ts, decreasing = TRUE),]  # Orden por ts descendente

  ultima_posicion_en_geocerca <- df_datos_sin_paradas_duplicadas[1,]

  tiempos_a_marquesinas_restantes <- df_tiempos[df_tiempos$ID_PARADA == ultima_posicion_en_geocerca$ID_PARADA,]
  #tiempos_a_marquesinas_restantes <- tiempos_a_marquesinas_restantes[,tiempos_a_marquesinas_restantes[1,] > 0]
  tiempos_a_marquesinas_restantes[,tiempos_a_marquesinas_restantes[1,] < 0] <- "-"
  tiempos_a_marquesinas_restantes[,tiempos_a_marquesinas_restantes[1,] == 0] <- "-"

  # Suma de 5' si tienen tienmpo asignado a las paradas iniciales, ya que el tiempo no es el de llegada, si no el de salida
  posicion_paradas_iniciales <- which(colnames(tiempos_a_marquesinas_restantes) %in% df_paradas_iniciales$name)
  for(i in posicion_paradas_iniciales){
    if(tiempos_a_marquesinas_restantes[1,i] != "-"){
      tiempos_a_marquesinas_restantes[1,i] <- tiempos_a_marquesinas_restantes[1,i] + 1
    }
  }



  #------------------------------------------------------------------------------
  # 4) - CÁLCULO TIEMPOS LLEGADA EN SENTIDO CONTRARIO
  #-----------------------------------------------------------------------------
  # Este bus es el encargado de escribir el tiempo de llegada futuro en el sentido contrario que circula ahora

  # Referencia tiempos sentido contrario
  if(sentido == 0){
    sentido_contrario <- 1
  }else{
    sentido_contrario <- 0
  }

  if(linea == 1){
    if(sentido_contrario == 0){
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L1.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L1.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }else if(linea == 2){
    if(sentido_contrario == 0){
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L2.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L2.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }else if(linea == 3){
    if(sentido_contrario == 0){
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L3.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      df_tiempos_contrario <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L3.csv",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }


  if(parada_destino == "P. La Data"){
    parada_destino <- "Gabriel y Galán 1_bajada"
  }
  if(parada_destino == "PIR Los Monjes"){
    parada_destino <- "PIR Los Monjes 2"
  }

  tiempos_a_marquesinas_restantes_contrario <- df_tiempos_contrario[grep(parada_destino,df_tiempos_contrario$NOMBRE_PARADA_GEOCERCA),]
  # Suma de tiempo máximo sentido actual a tiempo sentido contrario
  max_tiempo_sentido_actual <- max(as.numeric(tiempos_a_marquesinas_restantes[,3:ncol(tiempos_a_marquesinas_restantes)]), na.rm = TRUE)
  tiempos_a_marquesinas_restantes_contrario[,3:ncol(tiempos_a_marquesinas_restantes_contrario)] <- tiempos_a_marquesinas_restantes_contrario[,3:ncol(tiempos_a_marquesinas_restantes_contrario)] + max_tiempo_sentido_actual
  tiempos_a_marquesinas_restantes_contrario[,tiempos_a_marquesinas_restantes_contrario[1,] < 0] <- "-"
  tiempos_a_marquesinas_restantes_contrario[,tiempos_a_marquesinas_restantes_contrario[1,] == 0] <- "-"

  # Suma de 5' para todas las paradas del sentido contrario (siguiente bus)
  posicion_paradas_iniciales <- which(colnames(tiempos_a_marquesinas_restantes_contrario) %in% df_paradas_iniciales$name)
  for(i in 3:ncol(tiempos_a_marquesinas_restantes_contrario)){
    if(tiempos_a_marquesinas_restantes_contrario[1,i] != "-"){
      tiempos_a_marquesinas_restantes_contrario[1,i] <- tiempos_a_marquesinas_restantes_contrario[1,i] + 1
    }
  }





  #------------------------------------------------------------------------------
  # 5) - ACTUALIZACIÓN ATRIBUTOS SENTIDO ACTUAL
  #-----------------------------------------------------------------------------

  # GET ACTIVOS TIPO "PARADA"
  url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_activos <- df[df$data.type == "parada",]

  # Filtrado por nombre marquesinas restantes en línea bus
  nombre_paradas_objetivo <- colnames(tiempos_a_marquesinas_restantes)[3:ncol(tiempos_a_marquesinas_restantes)]
  df_activos <- df_activos[which(df_activos$data.name %in% nombre_paradas_objetivo),]

  # TENGO EL ID DEL ACTIVO SOBRE EL QUE TENGO QUE ACTUALIZAR LOS ATRIBUTOS DE TIEMPO. SÉ EN QUE LÍNEA ESTOY Y SÉ EL SENTIDO, POR LO QUE TENGO QUE ACTUALIZAR LOS ATRIBUTOS DE MI LINEA, Y MI SENTIDO
  df_activos <- df_activos[order(df_activos$data.name, decreasing = FALSE),]  # Orden activos por nombre
  orden_columnas_tiempos <- colnames(tiempos_a_marquesinas_restantes)[3:ncol(tiempos_a_marquesinas_restantes)]
  orden_columnas_tiempos <- order(orden_columnas_tiempos, decreasing = FALSE)
  orden_columnas_tiempos <- 2 + orden_columnas_tiempos
  tiempos_a_marquesinas_restantes <- tiempos_a_marquesinas_restantes[,c(1:2,orden_columnas_tiempos)] # Orden columnas tiempos por nombre para coincidir con df_activos
  # Asignación de indentificador bus en parada actual
  tiempos_a_marquesinas_restantes[,which(colnames(tiempos_a_marquesinas_restantes) %in% tiempos_a_marquesinas_restantes$NOMBRE_PARADA_GEOCERCA)] <- "En parada"
  #if(flag_ultimo_trayecto != TRUE){
  #  tiempos_a_marquesinas_restantes[tiempos_a_marquesinas_restantes == "-"] <- "> 30 minutos"  # Ya ha pasado por esta parada y no es el último trayecto
  #}



  # ENVIO TELEMETRÍA A ACTIVO TIPO "PARADA" DE PASO POR PARADA. PERMITE RECOGER INDICADORES
  id <- df_activos$data.id$id[df_activos$data.name == tiempos_a_marquesinas_restantes$NOMBRE_PARADA_GEOCERCA]
  url_telemetria <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", id, "/timeseries/ANY?scope=ANY",sep = "")
  json_envio_plataforma <- paste('{"linea_',linea,'":', 1,
                                 '}',sep = "")

  post <- httr::POST(url = url_telemetria,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )





  # CÁLCULO DE AFORO Y ESCRITURA EN TELEMETRÍA
  #keys <- URLencode(c("Aforo"))
  #url_thb_fechas <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/",id_dispositivo,"/values/timeseries?limit=10000&keys=",keys,"&startTs=",fecha_1,"&endTs=",fecha_2,sep = "")
  #peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  # Tratamiento datos. De raw a dataframe
  #df <- jsonlite::fromJSON(rawToChar(peticion$content))
  #df <- as.data.frame(df)
  #df <- df[,-c(3,5)]

  #colnames(df) <- c("ts","Aforo")
  #df$fecha_time <- as.POSIXct(as.numeric(df$ts)/1000, origin = "1970-01-01")
  #aforo <- df$Aforo[1]
  #pos <- which(colnames(df_tiempos) %in% ultima_posicion_en_geocerca$NOMBRE_PARADA_GEOCERCA)
  #if(pos == 3){pos <- 4}
  #tiempo_a_restar <- df_tiempos[df_tiempos$NOMBRE_PARADA_GEOCERCA == ultima_posicion_en_geocerca$NOMBRE_PARADA_GEOCERCA, (pos - 1)]
  #ref_tiempo <- df$fecha_time[1]
  #minute(ref_tiempo) <- minute(df$fecha_time[1]) + tiempo_a_restar
  #df <- df[df$fecha_time > ref_tiempo,]
  #if(length(unique(df$Aforo)) > 1){
  #  df <- df[!duplicated(df$Aforo),]
  #  flujo <- as.numeric(df$Aforo[1]) - as.numeric(df$Aforo[2])
  #}else{
  #  flujo <- as.numeric(df$Aforo[1]) - as.numeric(df$Aforo[2])
  #}

  #id <- df_activos$data.id$id[df_activos$data.name == tiempos_a_marquesinas_restantes$NOMBRE_PARADA_GEOCERCA]
  #url_telemetria <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", id, "/timeseries/ANY?scope=ANY",sep = "")
  #json_envio_plataforma <- paste('{"Aforo":',aforo,',','"Flujo":',flujo,
  #                               '}',sep = "")

  #post <- httr::POST(url = url_telemetria,
  #                   add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
  #                   body = json_envio_plataforma,
  #                   verify= FALSE,
  #                   encode = "json",verbose()
  #)




  # RECOGIDA DE VALOR ATRIBUTOS EN MARQUESINAS OBJETIVO PARA DECIDIR SI ESCRIBIR O NO
  url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_valor_atributos_actual <- df[df$data.type == "parada",]
  df_valor_atributos_actual <- df_valor_atributos_actual[which(df_valor_atributos_actual$data.name %in% nombre_paradas_objetivo),]

  # Atributo tiempo de llegada plataforma. Primer atributo, por lo que vuelta actual
  if(linea == 1){
    keys <- URLencode(c("tiempo_llegada_linea_1"))
  }else if(linea == 2){
    keys <- URLencode(c("tiempo_llegada_linea_2"))
  }else if(linea == 3){
    keys <- URLencode(c("tiempo_llegada_linea_3"))
  }

  df_tiempos_actuales <- data.frame()
  nombre_parada <- c()
  for(i in 1:nrow(df_valor_atributos_actual)){
    nombre_parada <- c(nombre_parada, df_valor_atributos_actual$data.name[i])
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_valor_atributos_actual$data.id$id[i],"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_tiempos_actuales <- rbind(df_tiempos_actuales,df)
  }
  df_tiempos_actuales$name <- nombre_parada
  df_tiempos_actuales <- df_tiempos_actuales[order(df_tiempos_actuales$name, decreasing = FALSE),]

  #  Atributo tiempo de llegada 2 plataforma. Segundo atributo, para gestión siguente autobús
  if(linea == 1){
    keys <- URLencode(c("tiempo_2_llegada_linea_1"))
  }else if(linea == 2){
    keys <- URLencode(c("tiempo_2_llegada_linea_2"))
  }else if(linea == 3){
    keys <- URLencode(c("tiempo_2_llegada_linea_3"))
  }

  df_tiempos_actuales_2 <- data.frame()
  nombre_parada <- c()
  for(i in 1:nrow(df_valor_atributos_actual)){
    nombre_parada <- c(nombre_parada, df_valor_atributos_actual$data.name[i])
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_valor_atributos_actual$data.id$id[i],"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_tiempos_actuales_2 <- rbind(df_tiempos_actuales_2,df)
  }
  df_tiempos_actuales_2$name <- nombre_parada
  df_tiempos_actuales_2 <- df_tiempos_actuales_2[order(df_tiempos_actuales_2$name, decreasing = FALSE),]


  # 4) Actualización atributos tiempo_llegada_linea_x. Actualización tiempos de llegada autobús actual.
  for(i in 1:nrow(df_activos)){

    tiempo_atributo_2 <- FALSE  # Flag escritura segundo atributo de plataforma

    # CUANDO LLEGA A LA FILA DE LA PARADA EN LA QUE SE ENCUENTRA, SE REGISTRA UN VALOR = en_parada
    if(tiempos_a_marquesinas_restantes[,(i+2)] == "En parada"){
      if(flag_ultimo_trayecto == TRUE){
        tiempo_atributos <- "-"
      }else{ # Decido si volver a escribir en parada si el valor de plataforma es != "-" y en base al tiempo que haya pasado desde el último valor de "En parada" en atributo plataforma
        if(df_tiempos_actuales$value[i] == "-"){
          tiempo_atributos <-tiempos_a_marquesinas_restantes[,(i+2)]
        }else{
          tiempo_actualizacion_atributo_en_segundos <- as.numeric(difftime(Sys.time(),as.POSIXct(as.numeric(as.character(df_tiempos_actuales$lastUpdateTs[i]))/1000, origin="1970-01-01", tz="GMT-1"),units = "secs"))
          if(tiempo_actualizacion_atributo_en_segundos > 20){ # si > 20 segundos, escribo el siguiente tiempo
            tiempo_atributos <- df_tiempos_actuales_2$value[i]
            tiempo_atributo_2 <- as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales_2$value[i])) *2
          }else{
            tiempo_atributos <- "En parada"
          }
        }
      }
    }else{ # El valor del tiempo restante es numérico o "-"
      if(df_tiempos_actuales$value[i] == "En parada"){ # Si en la plataforma está el registro de "En parada"
        if(flag_ultimo_trayecto == TRUE){  # Si es el último trayecto, escribo el valor "-" para desasignar tiempo
          tiempo_atributos <- "-"
        }else{
          if(!grepl("\\d", tiempos_a_marquesinas_restantes[,(i+2)]) | tiempos_a_marquesinas_restantes[,(i+2)] == "> 30 minutos"){ # Si el bus actual no tiene número para esa parada, compruebo al valor del atributo 2
            tiempo_atributo_2 <- TRUE
            if(grepl("\\d", df_tiempos_actuales_2$value[i]) & df_tiempos_actuales_2$value[i] != "> 30 minutos"){  # Si el valor del segundo atributo es númerico y no es > 30 mins. Escribo este valor.
              if(as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales_2$value[i])) > 15){ # > 15 minutos, si no, es que solo hay 1 autobús
                tiempo_atributos <- df_tiempos_actuales_2$value[i]
                tiempo_atributo_2 <- "-"  # Asigno > 30 mins a tiempo atributo 2
              }else{
                tiempo_atributos <- df_tiempos_actuales_2$value[i]
                tiempo_atributo_2 <- "-"  # Asigno - mins a tiempo atributo 2
              }
            }else{ # No tiene número o tiene asignación de > 30 minutos
              tiempo_atributos <- "-"
              tiempo_atributo_2 <- "-"  # Asigno > 60 mins a tiempo atributo 2
            }
          }else{  # Si hay número a registrar
            if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
              tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
            }else{
              tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
            }
          }
        }
      }else{  # Si valor en platafroma != "En parada"
        # Comprobación de si existe ya un tiempo asignado en plataforma
        if(grepl("\\d", df_tiempos_actuales$value[i]) & df_tiempos_actuales$value[i] != "> 30 minutos" & df_tiempos_actuales$value[i] != "> 30 minutos minutos"){ # si hay número en plataforma, salto a comprobar si el bus actual tiene número asignado para esa parada
          if(!grepl("\\d", tiempos_a_marquesinas_restantes[,(i+2)]) | tiempos_a_marquesinas_restantes[,(i+2)] == "> 30 minutos"){ # Si el bus actual no tiene número para esa parada, compruebo si es último trayecto y si no es, el valor del segundo tiempo en plataforma
            if(flag_ultimo_trayecto == TRUE){  # Si es el último trayecto, escribo el valor "-" para desasignar tiempo
              tiempo_atributos <- "-"
            }else{
              if(df_tiempos_actuales_2$value[i] == "En parada"){
                tiempo_atributo_2 <- TRUE
                tiempo_atributos <- tiempos_a_marquesinas_restantes_contrario[,(i+2)]
                tiempo_atributo_2 <- tiempos_a_marquesinas_restantes_contrario[,(i+2)] + 20
              }else{
                if(df_tiempos_actuales_2$value[i] != "-"){
                  if(as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales$value[i])) >= as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales_2$value[i]))){ # Si el valor de tiempo del atributo 1 > atributo 2, cambio valores
                    tiempo_atributo_2 <- TRUE
                    tiempo_atributos <- df_tiempos_actuales_2$value[i]
                    tiempo_atributo_2 <- as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales_2$value[i])) + 20
                  }else{
                    next
                  }
                }else{
                  next
                }
              }
            }
          }else{  # El bus actual tiene número
            if(as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales$value[i])) < tiempos_a_marquesinas_restantes[,(i+2)]){ # Si el número que hay ahora registrado en plataforma es menor que el del presente bus, compruebo momento de última actualización.
              diferencia_tiempo_en_minutos <- as.numeric(difftime(Sys.time(),as.POSIXct(as.numeric(as.character(df_tiempos_actuales$lastUpdateTs[i]))/1000, origin="1970-01-01", tz="GMT-1"),units = "mins"))
              if(diferencia_tiempo_en_minutos >= 5){
                if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
                  tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
                }else{
                  tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
                }
              }else{
                next
              }
            }
          }
        }
      }

      if(tiempo_atributo_2 == FALSE){  # Escribo alguno de los valores del bus actual, no del atributo 2 de plataforma.
        if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
        }else if(tiempos_a_marquesinas_restantes[,(i+2)] != "> 30 minutos" & grepl("\\d", tiempos_a_marquesinas_restantes[,(i+2)])){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
        }else{
          tiempo_atributos <- tiempos_a_marquesinas_restantes[,(i+2)]
        }
      }

    } # Cierre else de no tiene valor == "En parada"


    # Escritura en atributos
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", df_activos$data.id$id[i], "/SERVER_SCOPE",sep = "")
    if(linea == 1){
      json_envio_plataforma <- paste('{"tiempo_llegada_linea_1":"', tiempo_atributos,'"',
                                     '}',sep = "")
      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )

      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_1":"', tiempo_atributos,'"',
                                       '}',sep = "")

         post <- httr::POST(url = url,
                           add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                           body = json_envio_plataforma,
                           verify= FALSE,
                           encode = "json",verbose()
        )
      }
    }

    if(linea == 2){

      json_envio_plataforma <- paste('{"tiempo_llegada_linea_2":"', tiempo_atributos,'"',
                                     '}',sep = "")

      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )

      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_2":"', tiempo_atributos,'"',
                                       '}',sep = "")

        post <- httr::POST(url = url,
                           add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                           body = json_envio_plataforma,
                           verify= FALSE,
                           encode = "json",verbose()
        )
      }
    }

    if(linea == 3){

      json_envio_plataforma <- paste('{"tiempo_llegada_linea_3":"', tiempo_atributos,'"',
                                     '}',sep = "")
      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )

      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_3":"', tiempo_atributos,'"',
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










  #------------------------------------------------------------------------------
  # 6) - ACTUALIZACIÓN ATRIBUTOS SENTIDO CONTRARIO
  #-----------------------------------------------------------------------------

  # GET ACTIVOS
  url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_activos <- df[df$data.type == "parada",]


  # Filtrado por nombre marquesinas restantes en línea bus
  nombre_paradas_objetivo <- colnames(tiempos_a_marquesinas_restantes_contrario)[3:ncol(tiempos_a_marquesinas_restantes_contrario)]
  df_activos <- df_activos[which(df_activos$data.name %in% nombre_paradas_objetivo),]

  # TENGO EL ID DEL ACTIVO SOBRE EL QUE TENGO QUE ACTUALIZAR LOS ATRIBUTOS DE TIEMPO. SÉ EN QUE LÍNEA ESTOY Y SÉ EL SENTIDO, POR LO QUE TENGO QUE ACTUALIZAR LOS ATRIBUTOS DE MI LINEA, Y MI SENTIDO

  df_activos <- df_activos[order(df_activos$data.name, decreasing = FALSE),]  # Orden activos por nombre
  orden_columnas_tiempos <- colnames(tiempos_a_marquesinas_restantes_contrario)[3:ncol(tiempos_a_marquesinas_restantes_contrario)]
  orden_columnas_tiempos <- order(orden_columnas_tiempos, decreasing = FALSE)
  orden_columnas_tiempos <- 2 + orden_columnas_tiempos
  tiempos_a_marquesinas_restantes_contrario <- tiempos_a_marquesinas_restantes_contrario[,c(1:2,orden_columnas_tiempos)] # Orden columnas tiempos por nombre para coincidir con df_activos
  if(nrow(tiempos_a_marquesinas_restantes_contrario) > 1) {tiempos_a_marquesinas_restantes_contrario <- tiempos_a_marquesinas_restantes_contrario[1,]}
  # Asignación de indentificador bus en parada actual
  #tiempos_a_marquesinas_restantes_contrario[,which(colnames(tiempos_a_marquesinas_restantes_contrario) %in% tiempos_a_marquesinas_restantes_contrario$NOMBRE_PARADA_GEOCERCA)] <- "En parada"
  #tiempos_a_marquesinas_restantes_contrario[tiempos_a_marquesinas_restantes_contrario == "-"] <- "> 30 minutos"

  # RECOGIDA DE VALOR ATRIBUTOS EN MARQUESINAS OBJETIVO PARA DECIDIR SI ESCRIBIR O NO
  url_thb <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_valor_atributos_actual <- df[df$data.type == "parada",]
  df_valor_atributos_actual <- df_valor_atributos_actual[which(df_valor_atributos_actual$data.name %in% nombre_paradas_objetivo),]

  # Atributo tiempo de llegada plataforma
  if(linea == 1){
    keys <- URLencode(c("tiempo_llegada_linea_1"))
  }else if(linea == 2){
    keys <- URLencode(c("tiempo_llegada_linea_2"))
  }else if(linea == 3){
    keys <- URLencode(c("tiempo_llegada_linea_3"))
  }

  df_tiempos_actuales_contrario <- data.frame()
  nombre_parada <- c()
  for(i in 1:nrow(df_valor_atributos_actual)){
    nombre_parada <- c(nombre_parada, df_valor_atributos_actual$data.name[i])
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_valor_atributos_actual$data.id$id[i],"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_tiempos_actuales_contrario <- rbind(df_tiempos_actuales_contrario,df)
  }
  df_tiempos_actuales_contrario$name <- nombre_parada
  df_tiempos_actuales_contrario <- df_tiempos_actuales_contrario[order(df_tiempos_actuales_contrario$name, decreasing = FALSE),]


  #  Atributo tiempo de llegada 2 plataforma
  if(linea == 1){
    keys <- URLencode(c("tiempo_2_llegada_linea_1"))
  }else if(linea == 2){
    keys <- URLencode(c("tiempo_2_llegada_linea_2"))
  }else if(linea == 3){
    keys <- URLencode(c("tiempo_2_llegada_linea_3"))
  }

  df_tiempos_actuales_2_contrario <- data.frame()
  nombre_parada <- c()
  for(i in 1:nrow(df_valor_atributos_actual)){
    nombre_parada <- c(nombre_parada, df_valor_atributos_actual$data.name[i])
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_valor_atributos_actual$data.id$id[i],"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df_tiempos_actuales_2_contrario <- rbind(df_tiempos_actuales_2_contrario,df)
  }
  df_tiempos_actuales_2_contrario$name <- nombre_parada
  df_tiempos_actuales_2_contrario <- df_tiempos_actuales_2_contrario[order(df_tiempos_actuales_2_contrario$name, decreasing = FALSE),]


  # 4) Actualización atributos tiempo_2_llegada_linea_x. Actualiza el atributo del tiempo de llegada del bus de sentido contrario (segundo bus del sentido contrario)
  for(i in 1:nrow(df_activos)){

    flag_escritura_primer_atributo <- FALSE

    if(flag_ultimo_trayecto == TRUE){
      tiempo_atributos <- "-"
    }else{
      if(!grepl("\\d", df_tiempos_actuales_2_contrario$value)[i] | df_tiempos_actuales_2_contrario$value[i] == "> 30 minutos" | df_tiempos_actuales_2_contrario$value[i] == "> 30 minutos minutos"){  # Si atributo en plataforma no tiene tiempo asignado escribo, el tiempo de llegada
        if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] == 1){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minuto", sep = "")
        }else if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "> 30 minutos" & tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "En parada"){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
        }else{
          tiempo_atributos <- tiempos_a_marquesinas_restantes_contrario[,(i+2)]
        }
      }else{  # El atributo 2 en plataforma tiene tiempo asignado, tengo que decidir si escribo o no en el primer atributo. En el segundo atributo sí escribo
        # Escritura en segundo atributo
        if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] == 1){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minuto", sep = "")
        }else if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "> 30 minutos" & tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "En parada"){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
        }else{
          tiempo_atributos <- tiempos_a_marquesinas_restantes_contrario[,(i+2)]
        }

        # Decido si escribir en el primer atributo contrario en base aL valor del atributo
        if(df_tiempos_actuales_contrario$value[i] == "-" | df_tiempos_actuales_contrario$value[i] == "> 30 minutos"){
          flag_escritura_primer_atributo <- TRUE
          tiempo_atributos <- paste(round(tiempos_a_marquesinas_restantes_contrario[,(i+2)]*1.5), " minutos", sep = "")
          tiempo_atributo_tiempo_1 <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
        }else{
          # Decido si escribir en el primer atributo contrario en base a al diferencia de tiempos de los atributos contrarios
          if(df_tiempos_actuales_contrario$value[i] != "En parada"){
            if(as.numeric(gsub(".*?([0-9]+).*", "\\1",df_tiempos_actuales_contrario$value[i])) >= as.numeric(gsub(".*?([0-9]+).*", "\\1",tiempos_a_marquesinas_restantes_contrario[(i+2)]))){ # Si el tiempo del atributo contrario en plataforma > que tiempo contrario
              flag_escritura_primer_atributo <- TRUE
              tiempo_atributos <- paste(round(tiempos_a_marquesinas_restantes_contrario[,(i+2)]*1.5), " minutos", sep = "")
              tiempo_atributo_tiempo_1 <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
            }else{
              # Decido si escribir o no en el primer atributo en base a último tiempo de actualización
              diferencia_tiempo_en_minutos <- as.numeric(difftime(Sys.time(),as.POSIXct(as.numeric(as.character(df_tiempos_actuales_contrario$lastUpdateTs[i]))/1000, origin="1970-01-01", tz="GMT-1"),units = "mins"))
              if(diferencia_tiempo_en_minutos >= 5){ # Si la diferencia de tiempo de actualización respecto el tiempo actual es > 5, escribo en primer atributo valor del segundo atributo, ya que solo hay 1 bus
                flag_escritura_primer_atributo <- TRUE
                tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)]*2, " minutos", sep = "")
                tiempo_atributo_tiempo_1 <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
              }
            }
          }else{
            # Decido si escribir o no en el primer atributo en base a último tiempo de actualización
            diferencia_tiempo_en_minutos <- as.numeric(difftime(Sys.time(),as.POSIXct(as.numeric(as.character(df_tiempos_actuales_contrario$lastUpdateTs[i]))/1000, origin="1970-01-01", tz="GMT-1"),units = "mins"))
            if(diferencia_tiempo_en_minutos >= 5){ # Si la diferencia de tiempo de actualización respecto el tiempo actual es > 5, escribo en primer atributo valor del segundo atributo, ya que solo hay 1 bus
              flag_escritura_primer_atributo <- TRUE
              tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)]*2, " minutos", sep = "")
              tiempo_atributo_tiempo_1 <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
            }
          }
        }
      }


      # Comprobación si voy a escribir en el primer atributo, si no, si este tiene == "En Parada", si lleva más de 40" le asigno tiempo contrario y a tiempo contrario *1,5
      if(flag_escritura_primer_atributo == FALSE){
        if(df_tiempos_actuales_contrario$value[i] == "En parada"){
          diferencia_tiempo_en_segundos<- as.numeric(difftime(Sys.time(),as.POSIXct(as.numeric(as.character(df_tiempos_actuales_contrario$lastUpdateTs[i]))/1000, origin="1970-01-01", tz="GMT-1"),units = "secs"))
          if(diferencia_tiempo_en_segundos >= 30){
            flag_escritura_primer_atributo <- TRUE
            tiempo_atributos <- paste(round(tiempos_a_marquesinas_restantes_contrario[,(i+2)]*1.5), " minutos", sep = "")
            tiempo_atributo_tiempo_1 <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
          }
        }
      }
    } # Cierre else de flag_ultimo_trayecto == TRUE


    # Escritura en atributos
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", df_activos$data.id$id[i], "/SERVER_SCOPE",sep = "")
    if(linea == 1){
      json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_1":"', tiempo_atributos,'"',
                                     '}',sep = "")

      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )

      if(flag_escritura_primer_atributo != FALSE){
        json_envio_plataforma <- paste('{"tiempo_llegada_linea_1":"', tiempo_atributo_tiempo_1,'"',
                                       '}',sep = "")

        post <- httr::POST(url = url,
                           add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                           body = json_envio_plataforma,
                           verify= FALSE,
                           encode = "json",verbose()
        )
      }
    }

    if(linea == 2){

      json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_2":"', tiempo_atributos,'"',
                                     '}',sep = "")

      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )

      if(flag_escritura_primer_atributo != FALSE){
        json_envio_plataforma <- paste('{"tiempo_llegada_linea_2":"', tiempo_atributo_tiempo_1,'"',
                                       '}',sep = "")

        post <- httr::POST(url = url,
                           add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                           body = json_envio_plataforma,
                           verify= FALSE,
                           encode = "json",verbose()
        )
      }
    }

    if(linea == 3){

      json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_3":"', tiempo_atributos,'"',
                                     '}',sep = "")

      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )
      if(flag_escritura_primer_atributo != FALSE){
        json_envio_plataforma <- paste('{"tiempo_llegada_linea_3":"', tiempo_atributo_tiempo_1,'"',
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

  return(1)
}
