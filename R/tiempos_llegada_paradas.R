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

  linea_original <- linea
  flag_ultimo_trayecto <- grepl("Último", linea_original)

  # Datos recogidos por plataforma
  id_dispositivo <- as.character(id_dispositivo)
  if(grepl("Último", linea)){
    linea <- as.numeric(gsub(" - Último trayecto","",linea))
  }else if(linea == "Fuera de servicio"){
    return(0)
  }else{
    linea <- as.numeric(linea)
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


  print("------------ Entro a petición token de acceso ----------------")



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



  print("------------ Éxito petición token de acceso ----------------")


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

  print("------- REALIZADA PETICIÓN RECOGIDA DATOS BÚS-----------------")

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
    print("------------ NO HAY DATOS DEL AUTOBÚS EN LA ÚLTIMA HORA --------------")
    return(0)
  }

  print("------------ RECIBIDOS DATOS DEL AUTOBÚS --------------")


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


  # CÁLCULO SENTIDO
  # Cálculo sentido si está en parada de inicio
  if(nrow(df_datos_sin_paradas_duplicadas) != 0){  # El bus se encuentra en una parada de inicio
    id_parada_inicial <- df_datos_sin_paradas_duplicadas$ID_PARADA[1]
    if(linea == 1){
      if(id_parada_inicial == 55){ # La Data
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 68 | id_parada_inicial == 47){ # Hospital Psiquiatrico o SEPEI Bomberos
        sentido <- 1  # Subiendo
      }
    }else if(linea == 2){
      if(id_parada_inicial == 48){ # Hospital virgen del puerto
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 39){ # Renfe / Estación de tren
        sentido <- 1  # Subiendo
      }
    }else if(linea == 3){
      if(id_parada_inicial == 48){ # Hospital virgen del puerto
        sentido <- 0  # Bajando
      }else if(id_parada_inicial == 69 | id_parada_inicial == 17){ # PIR los monges o Carretera Trujillo
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
        if(id_parada_inicial == 48 | id_parada_inicial == 55 | id_parada_inicial == 47 | id_parada_inicial == 68 | id_parada_inicial == 39 | id_parada_inicial == 69){ # Paradas de salida
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
      parada_destino <- "Hospital Psiquiátrico"
    }else{
      parada_destino <- "Polígono La Data"
    }
  }else if(linea == 2){
    if(sentido == 0){
      parada_destino <- "Renfe"
    }else{
      parada_destino <- "Hospital Virgen del Puerto"
    }
  }else if(linea == 3){
    if(sentido == 0){
      parada_destino <- "PIR Los Monges"
    }else{
      parada_destino <- "Hospital Virgen del Puerto"
    }
  }

  url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/DEVICE/", id_dispositivo, "/SERVER_SCOPE",sep = "")
  json_envio_plataforma <- paste('{"parada_destino":"', parada_destino,'",', '"sentido":', sentido,
                                 '}',sep = "")
  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )








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
      tiempos_a_marquesinas_restantes[1,i] <- tiempos_a_marquesinas_restantes[1,i] + 5
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


  if(parada_destino == "Renfe"){ parada_destino <- "Estación de Tren"}

  tiempos_a_marquesinas_restantes_contrario <- df_tiempos_contrario[grep(parada_destino,df_tiempos_contrario$NOMBRE_PARADA_GEOCERCA),]
  # Suma de tiempo máximo sentido actual a tiempo sentido contrario
  max_tiempo_sentido_actual <- max(as.numeric(tiempos_a_marquesinas_restantes[,3:ncol(tiempos_a_marquesinas_restantes)]), na.rm = TRUE)
  tiempos_a_marquesinas_restantes_contrario[,3:ncol(tiempos_a_marquesinas_restantes_contrario)] <- tiempos_a_marquesinas_restantes_contrario[,3:ncol(tiempos_a_marquesinas_restantes_contrario)] + max_tiempo_sentido_actual
  tiempos_a_marquesinas_restantes_contrario[,tiempos_a_marquesinas_restantes_contrario[1,] < 0] <- "-"
  tiempos_a_marquesinas_restantes_contrario[,tiempos_a_marquesinas_restantes_contrario[1,] == 0] <- "-"

  # Suma de 5' si tienen tienmpo asignado a las paradas iniciales, ya que el tiempo no es el de llegada, si no el de salida
  posicion_paradas_iniciales <- which(colnames(tiempos_a_marquesinas_restantes_contrario) %in% df_paradas_iniciales$name)
  for(i in posicion_paradas_iniciales){
    if(tiempos_a_marquesinas_restantes_contrario[1,i] != "-"){
      tiempos_a_marquesinas_restantes_contrario[1,i] <- tiempos_a_marquesinas_restantes_contrario[1,i] + 5
    }
  }





  #------------------------------------------------------------------------------
  # 5) - ACTUALIZACIÓN ATRIBUTOS SENTIDO ACTUAL
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
  # Asignación de indentificador bus en parada actual
  tiempos_a_marquesinas_restantes[,which(colnames(tiempos_a_marquesinas_restantes) %in% tiempos_a_marquesinas_restantes$NOMBRE_PARADA_GEOCERCA)] <- "En parada"
  tiempos_a_marquesinas_restantes[tiempos_a_marquesinas_restantes == "-"] <- "> 30 minutos"



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




  #  Atributo tiempo de llegada 2 plataforma

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



  # 4) Creación atributos tiempo_llegada_linea_x
  for(i in 1:nrow(df_activos)){

    tiempo_atributo_2 <- FALSE
    Sys.sleep(2)
    print("Tiempo a marquesina restante")
    print(tiempos_a_marquesinas_restantes[,(i+2)])
    print("Tiempo plataforma")
    print(df_tiempos_actuales$value[i])

    # CUANDO LLEGA A LA FILA DE LA PARADA EN LA QUE SE ENCUENTRA, SE REGISTRA UN VALOR = en_parada
    if(tiempos_a_marquesinas_restantes[,(i+2)] == "En parada"){
      tiempo_atributos <- "En parada"
    }else{
      # Comprobación de si en la plataforma está asignado el valor "En parada"
      if(df_tiempos_actuales$value[i] == "En parada"){ # Si en la plataforma está el registro de "En parada"
        if(!grepl("\\d", tiempos_a_marquesinas_restantes[,(i+2)]) | tiempos_a_marquesinas_restantes[,(i+2)] == "> 30 minutos"){ # Si el bus actual no tiene número para esa parada, compruebo al valor del atributo 2
          if(grepl("\\d", df_tiempos_actuales_2$value[i]) | df_tiempos_actuales_2$value[i] != "> 30 minutos"){  # Si el valor del segundo atributo es númerico y no es > 30 mins. Escribo este valor.
            tiempo_atributos <- df_tiempos_actuales_2$value[i]
            tiempo_atributo_2 <- "> 30 minutos"  # Asigno > 30 mins a tiempo atributo 2
          }
        }else{  # Si hay número a registrar
          if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
            tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
          }else{
            tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
          }
        }
      }else{  # Si valor en platafroma != "En parada"
        # Comprobación de si existe ya un tiempo asignado en plataforma
        if(grepl("\\d", df_tiempos_actuales$value[i]) & df_tiempos_actuales$value[i] != "> 30 minutos" & df_tiempos_actuales$value[i] != "> 30 minutos minutos"){ # si hay número en plataforma, salto a comprobar si el bus actual tiene número asignado para esa parada
          if(!grepl("\\d", tiempos_a_marquesinas_restantes[,(i+2)]) | tiempos_a_marquesinas_restantes[,(i+2)] == "> 30 minutos"){ # Si el bus actual no tiene número para esa parada, salto a la siguiente vuelta
            next
          }else{
            if(as.numeric(gsub(" .*","",df_tiempos_actuales$value)[i]) < tiempos_a_marquesinas_restantes[,(i+2)]){ # Si el número que hay ahora registrado en plataforma es menor que el del presente bus, salto.
              next
            }
          }
        }
      }
      if(tiempos_a_marquesinas_restantes[,(i+2)] == 1){
        tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minuto", sep = "")
      }else if(tiempos_a_marquesinas_restantes[,(i+2)] != "> 30 minutos"){
        tiempo_atributos <- paste(tiempos_a_marquesinas_restantes[,(i+2)], " minutos", sep = "")
      }else{
        tiempo_atributos <- tiempos_a_marquesinas_restantes[,(i+2)]
      }
    } # Cierre else de no tiene valor == "En parada"

    print("------------------------------")
    print("Tiempo atributos")
    print(tiempo_atributos)
    print("------------------------------")


    # Escritura en atributos
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", df_activos$data.id$id[i], "/SERVER_SCOPE",sep = "")
    if(linea == 1){
      json_envio_plataforma <- paste('{"tiempo_llegada_linea_1":"', tiempo_atributos,'"',
                                     '}',sep = "")
      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_1":"', tiempo_atributos,'"',
                                       '}',sep = "")
      }

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
      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_2":"', tiempo_atributos,'"',
                                       '}',sep = "")
      }

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
      if(tiempo_atributo_2 != FALSE){
        json_envio_plataforma <- paste('{"tiempo_2_llegada_linea_3":"', tiempo_atributos,'"',
                                       '}',sep = "")
      }

      post <- httr::POST(url = url,
                         add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                         body = json_envio_plataforma,
                         verify= FALSE,
                         encode = "json",verbose()
      )
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
  # Asignación de indentificador bus en parada actual
  tiempos_a_marquesinas_restantes_contrario[,which(colnames(tiempos_a_marquesinas_restantes_contrario) %in% tiempos_a_marquesinas_restantes_contrario$NOMBRE_PARADA_GEOCERCA)] <- "En parada"
  tiempos_a_marquesinas_restantes_contrario[tiempos_a_marquesinas_restantes_contrario == "-"] <- "> 30 minutos"



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




  # 4) Creación atributos tiempo_llegada_linea_x
  for(i in 1:nrow(df_activos)){

    tiempo_atributo_2 <- FALSE
    Sys.sleep(2)
    print("Tiempo a marquesina restante CONTRARIO")
    print(tiempos_a_marquesinas_restantes_contrario[,(i+2)])
    print("Tiempo plataforma ATRIBUTO 2 CONTRARIO")
    print(df_tiempos_actuales_2_contrario$value[i])


    if(!grepl("\\d", df_tiempos_actuales_2_contrario$value)[i] | df_tiempos_actuales_2_contrario$value[i] == "> 30 minutos" | df_tiempos_actuales_2_contrario$value[i] == "> 30 minutos minutos"){    # Si no tiene tiempo asignado escribo
      if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] == 1){
        tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minuto", sep = "")
      }else if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "> 30 minutos"){
        tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
      }else{
        tiempo_atributos <- tiempos_a_marquesinas_restantes_contrario[,(i+2)]
      }
    }else{  # Tiene tiempo asignado, tengo que ver si escribo o no
      if(as.numeric(gsub(" .*","",df_tiempos_actuales_2_contrario$value)[i]) < tiempos_a_marquesinas_restantes_contrario[,(i+2)]){ # Si el tiempo en plataforma es menor que el actual contrario, no escribo
        next
      }else{
        if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] == 1){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minuto", sep = "")
        }else if(tiempos_a_marquesinas_restantes_contrario[,(i+2)] != "> 30 minutos"){
          tiempo_atributos <- paste(tiempos_a_marquesinas_restantes_contrario[,(i+2)], " minutos", sep = "")
        }else{
          tiempo_atributos <- tiempos_a_marquesinas_restantes_contrario[,(i+2)]
        }
      }
    }


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
    }
  }
}
