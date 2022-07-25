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
#'
#' @export

tiempos_llegada_paradas <- function(id_dispositivo, linea){

  # Datos recogidos por plataforma
  id_dispositivo <- as.character(id_dispositivo)
  linea <- as.numeric(linea)

  # ------------------------------------------------------------------------------
  # 0) - REFERENCIA PARADAS
  # ------------------------------------------------------------------------------

  #df_paradas <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/paradas_bus_plasencia.csv", sep = ",")
  ficheros_en_ruta <- list.files(system.file('extdata', package = 'comparativaDescriptivos'), full.names = TRUE)
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
  # 1) - RECEPCIÓN GEOPOSICIONAMIENTO AUTOBUS
  # ------------------------------------------------------------------------------


  #fecha_1 <- "2022-07-24 13:55:00"
  #fecha_2 <- "2022-07-24 13:58:00"
  fecha_1 <- Sys.time() - 60*20 # Timestamp actual menos 20 mins
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
  df <- df[-grep(" km/h",df$spe),]
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
  # 2) - OBTENCIÓN DEL SENTIDO DEL AUTOBUS
  #-----------------------------------------------------------------------------
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

  # Bucle para cada uno de los registros de posición capturados del autobus
  ID_GEOCERCA <- c()
  ID_PARADA <- c()
  NOMBRE_PARADA_GEOCERCA <- c()
  for(i in 1:20){ # 20 primeras posiciones del autobús

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
      nombre_parada_detección_bus <- df_geocercas$nombre_parada[df_geocercas$id_geocerca == id_geocerca_actual]

      # Volcado en arrays
      ID_GEOCERCA <- c(ID_GEOCERCA, id_geocerca_actual) # Volcado id geocerca en array geocercas
      NOMBRE_PARADA_GEOCERCA <- c(NOMBRE_PARADA_GEOCERCA, nombre_parada_detección_bus) # Volcado nombre parada en array geocercas
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
  df_datos_sin_paradas_duplicadas <- df_datos_sin_paradas_duplicadas[order(df_datos_sin_paradas_duplicadas$ts, decreasing = FALSE),]  # Orden por ts


  # Get sentido en función de si ha partido de una de las paradas iniciales
  if(nrow(df_datos_sin_paradas_duplicadas) == 0){  # Cojo el sentido solo por la diferencia de longitudes ya que no he encontrado parada de inicio
    # Comprobación de sentido por diferencia de longitudes
    if((df_datos_bus$lon[1] - df_datos_bus$lon[nrow(df_datos_bus)]) > 0) { # Si la resta de la primera y última longitud en negativa, está bajando
      sentido <- 0
    }else{
      sentido <- 1
    }
  }else{
    id_parada_inicial <- df_datos_sin_paradas_duplicadas$ID_PARADA
    if(id_parada_inicial == 48 | id_parada_inicial == 55){ # La DATA o Hospital
      sentido_parada <- 0  # Bajando
    }else{
      sentido_parada <- 1  # Subiendo
    }

    # Comprobación de sentido por diferencia de longitudes
    if((df_datos_bus$lon[1] - df_datos_bus$lon[nrow(df_datos_bus)]) > 0) { # Si la resta de la primera y última longitud en negativa, está bajando
      sentido_lon <- 0
    }else{
      sentido_lon <- 1
    }

    sentido <- sentido_lon + sentido_parada
    if(sentido == 0 | sentido == 2){ # Se ha obtenido correctamente el sentido del bus
      if(sentido == 2){
        sentido <- 1
      }
    }else{
      return(0)  # No se puede asegurar el sentido del autobus
    }
  }



  # CÁLCULO DE TIEMPOS UNA VEZ CONOCIDO EL SENTIDO DEL BUS
  df_trabajo_paradas <- df_trabajo_paradas_linea_objetivo[df_trabajo_paradas_linea_objetivo$sentido == sentido | df_trabajo_paradas_linea_objetivo$sentido >=2,]






  #------------------------------------------------------------------------------
  # 3) - ACTUALIZACIÓN TIEMPOS DE LLEGADA A CADA PARADA
  #-----------------------------------------------------------------------------

  # Referencias tiempos
  if(linea == 1){
    if(sentido == 0){
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_bajada_L1.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L1",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_subida_L1.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L1",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      }
  }else if(linea == 2){
    if(sentido == 0){
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_bajada_L2.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L2",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_subida_L2.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L2",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }
  }else if(linea == 3){
    if(sentido == 0){
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_bajada_L3.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_bajada_L3",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
    }else{
      #df_tiempos <- read.csv("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/Datos lineas/Matriz_tiempos_minutos_paradas/matriz_tiempos_subida_L3.csv", sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
      df_tiempos <- read.csv(as.character(ficheros_en_ruta[grep("matriz_tiempos_subida_L3",ficheros_en_ruta)]), sep = ",", stringsAsFactors = FALSE, check.names = FALSE)
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

    print(i)

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
      nombre_parada_detección_bus <- df_geocercas$nombre_parada[df_geocercas$id_geocerca == id_geocerca_actual]

      # Volcado en arrays
      ID_GEOCERCA <- c(ID_GEOCERCA, id_geocerca_actual) # Volcado id geocerca en array geocercas
      NOMBRE_PARADA_GEOCERCA <- c(NOMBRE_PARADA_GEOCERCA, nombre_parada_detección_bus) # Volcado nombre parada en array geocercas
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
  df_datos_sin_paradas_duplicadas <- df_datos_sin_paradas_duplicadas[order(df_datos_sin_paradas_duplicadas$ts, decreasing = TRUE),]  # Orden por ts descendente

  ultima_posicion_en_geocerca <- df_datos_sin_paradas_duplicadas[1,]

  tiempos_a_marquesinas_restantes <- df_tiempos[df_tiempos$ID_PARADA == ultima_posicion_en_geocerca$ID_PARADA,]
  tiempos_a_marquesinas_restantes <- tiempos_a_marquesinas_restantes[,tiempos_a_marquesinas_restantes[1,] > 0]



  #------------------------------------------------------------------------------
  # 4) - ACTUALIZACIÓN ATRIBUTOS
  #-----------------------------------------------------------------------------


  # GET ACTIVOS
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

  # 4) Creación atributos tiempo_llegada_linea_x
  for(i in 1:nrow(df_activos)){
    print(i)
    if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
      tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
    }else{
      tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
    }

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
    }
  }

}
