#' @title Genera la referencia de paradas en base a activos tipo "parada" de plataforma y la referencia de tiempos de paso de bus (matriz origen destino)
#'
#' @description Genera la referencia de paradas en base a activos tipo "parada" de plataforma y la referencia de tiempos de paso de bus (matriz origen destino)
#'
#' @return json
#'
#'
#' @import httr
#' jsonlite
#' dplyr
#' sf
#' lubridate
#'
#' @export

referencia_paradas_y_tiempos_OD <- function(){

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
  # GET ACTIVOS TIPO "parada"
  # ------------------------------------------------------------------------------

  # 1) GET ACTIVOS
  url_thb_fechas <- "https://plataforma.plasencia.es/api/tenant/assets?pageSize=500&page=0"
  peticion <- GET(url_thb_fechas, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))

  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_activos <- df[df$data.type == "parada",]
  df_activos <- df_activos[order(df_activos$data.name, decreasing = FALSE),]


  # 2) GET ATRIBUTOS
  keys <- URLencode(c("id,latitud,longitud,linea_1,linea_2,linea_3,sentido"))  # Claves a recoger

  df_atributos <- data.frame()
  for(i in 1:nrow(df_activos)){
    url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/",df_activos$data.id$id[i],"/values/attributes/SERVER_SCOPE?keys=", keys,sep = "")
    peticion <- GET(url, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
    # Tratamiento datos. De raw a dataframe
    df <- jsonlite::fromJSON(rawToChar(peticion$content))
    df <- as.data.frame(df)
    df <- as.data.frame(t(df))
    colnames(df) <- df[2,]
    df <- df[-c(1,2),]
    df_atributos <- rbind(df_atributos,df)
  }
  # Paso a numérico
  for(i in 1:ncol(df_atributos)){
    df_atributos[,i] <- as.numeric(df_atributos[,i])
  }
  df <- df_atributos
  df$name <- df_activos$data.name
  df$row <- rep("-",nrow(df))
  df$osm_id <- rep("-",nrow(df))
  df <- df[,c("id","row","osm_id","name","latitud","longitud","linea_1","linea_2","linea_3","sentido")]  # Orden df


  # ------------------------------------------------------------------------------
  # GENERACIÓN CSV
  # ------------------------------------------------------------------------------

  #write.csv(df, "/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/CREACIÓN ATRIBUTOS/paradas_bus_plasencia.csv", row.names = FALSE)
  write.csv(df, "/root/paradas_bus_plasencia.csv", row.names = FALSE)



  # ------------------------------------------------------------------------------
  # ------------------------------------------------------------------------------
  # ------------------------------------------------------------------------------
  # CREACIÓN REFERENCIA DE TIEMPOS
  # ------------------------------------------------------------------------------
  # ------------------------------------------------------------------------------
  # ------------------------------------------------------------------------------
  library(sf)

  # ------------------------------------------------------------------------------
  # 0) - REFERENCIA PARADAS
  # ------------------------------------------------------------------------------

  df_paradas <- df

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
  # 1) - REFERENCIA IDS y FECHAS REFERENCIA RECORRIDOS
  # ------------------------------------------------------------------------------

  id_dispositivo_s <- c("77153920-0350-11ed-b4eb-0d97eeef399c","77153920-0350-11ed-b4eb-0d97eeef399c",
                        "88d9c0b0-3012-11ed-b1c7-45705c1f88e8","88d9c0b0-3012-11ed-b1c7-45705c1f88e8",
                        "0d8f5da0-e659-11ec-b4eb-0d97eeef399c","0d8f5da0-e659-11ec-b4eb-0d97eeef399c")

  fecha_1_s <- c("2022-11-29 11:18:45","2022-11-29 11:48:45","2022-11-29 12:10:00","2022-11-29 11:38:00","2022-11-29 11:47:30","2022-11-29 12:13:00")
  fecha_2_s <- c("2022-11-29 11:48:45","2022-11-29 12:14:00","2022-11-29 12:42:00","2022-11-29 12:11:00","2022-11-29 12:13:00","2022-11-29 12:53:00")

  linea_s <- c(3,3,1,1,2,2)
  sentido_s <- c(0,1,0,1,0,1)


  # BUCLE FOR PARA CÁLCULO REFERENCIAS DE TIEMPO POR SENTIDO
  for(i in 1:length(sentido_s)){
    print(i)

    # 1) GET DATOS BUS
    id_dispositivo <- id_dispositivo_s[i]
    sentido <- sentido_s[i]
    linea <- linea_s[i]
    fecha_1 <- format(as.numeric(as.POSIXct(fecha_1_s[i]))*1000,scientific = F)
    fecha_2 <- format(as.numeric(as.POSIXct(fecha_2_s[i]))*1000,scientific = F)

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



    # 2) GENERACIÓN GEOCERCAS POR LÍNEA Y SENTIDO
    #-----------------------------------------------------------------------------
    # Creación de geocercas
    if(linea == 1){
      df_trabajo_paradas <- df_paradas[df_paradas$linea_1 == 1 & (df_paradas$sentido == sentido | df_paradas$sentido >=2),]
    }else if(linea == 2){
      df_trabajo_paradas <- df_paradas[df_paradas$linea_2 == 1 & (df_paradas$sentido == sentido | df_paradas$sentido >=2),]
    }else{
      df_trabajo_paradas <- df_paradas[df_paradas$linea_3 == 1 & (df_paradas$sentido == sentido | df_paradas$sentido >=2),]
    }

    lat <- df_trabajo_paradas$latitud
    long <- df_trabajo_paradas$longitud

    # Agrupación de puntos en variable stores
    paradas_sfc <- st_sfc(st_multipoint(cbind(long, lat)), crs = 4326)   # Puntos paradas

    ID_GEOCERCA <- c()
    ID_PARADA <- c()
    NOMBRE_PARADA_GEOCERCA <- c()

    # PARADAS, NO HACE FALTA ITERAR
    # Cambio a UTM
    paradas_utm <- st_transform(paradas_sfc, "+proj=utm +zone=29")
    # Generación de geocercas en paradas
    paradas_separadas_id <- st_cast(paradas_utm, "POINT")
    geocercas <- st_buffer(paradas_separadas_id, 70)



    # 3) BUCLE POR CADA UNO DE LOS REGISTROS DE POSICIÓN DEL AUTOBUS Y GEOCERCAS
    # -----------------------------------------------------------------------------
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



    # 4) MATRIZ ORIGEN DESTINO
    # -----------------------------------------------------------------------------

    # Generación de nuevas columnas = nombre paradas
    for(i in 1:nrow(df_datos_sin_paradas_duplicadas)){
      df_datos_sin_paradas_duplicadas[, as.character(df_datos_sin_paradas_duplicadas$NOMBRE_PARADA_GEOCERCA[i])] <- replicate(nrow(df_datos_sin_paradas_duplicadas), NA)
    }

    # Calculo tiempos entre paradas
    for(col in 9:(ncol(df_datos_sin_paradas_duplicadas))){
      for(filas in 1:nrow(df_datos_sin_paradas_duplicadas)){

        tiempo <- df_datos_sin_paradas_duplicadas$fecha_time[filas]
        second(tiempo) <- second(tiempo) - 20

        diferencia_tiempo <- as.numeric(difftime(df_datos_sin_paradas_duplicadas$fecha_time[col-8], tiempo, units = "mins"))
        if(diferencia_tiempo < 1 & diferencia_tiempo > 0.5){
          df_datos_sin_paradas_duplicadas[filas,col] <- 1
        }else{
          df_datos_sin_paradas_duplicadas[filas,col] <- floor(as.numeric(difftime(df_datos_sin_paradas_duplicadas$fecha_time[col-8], tiempo, units = "mins")))
        }
      }
    }

    df_matriz_tiempos <- df_datos_sin_paradas_duplicadas[,7:ncol(df_datos_sin_paradas_duplicadas)]


    # 5) GENERACIÓN CSV
    # ------------------------------------------------------------------------------
    if(sentido == 1){
      sentido_string <- "subida"
    }else{
      sentido_string <- "bajada"
    }
    nombre_fichero_referencia_tiempos <- paste("matriz_tiempos_",sentido_string,"_L",linea,".csv",sep = "")
    print(nombre_fichero_referencia_tiempos)

    #write.csv(df_matriz_tiempos, paste("/home/kepa/TECH friendly/PROYECTOS/DTIS/DTI - Plasencia/Programas/C3 - Gestión autobuses/CREACIÓN ATRIBUTOS/", nombre_fichero_referencia_tiempos, sep=""), row.names = FALSE)
    write.csv(df_matriz_tiempos, paste("/root/", nombre_fichero_referencia_tiempos, sep=""), row.names = FALSE)

  } # Cierre bucle for
}
