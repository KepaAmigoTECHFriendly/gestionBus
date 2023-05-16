#' @title Comprueba si es festivo y escribe un 1 o 0 en un atributo "festivo" el activo "datos_lineas"
#'
#' @description Comprueba si es festivo y escribe un 1 o 0 en un atributo "festivo" el activo "datos_lineas"
#'
#' @return json
#'
#' @examples  festivo_plasencia()
#'
#' @import httr
#' jsonlite
#' dplyr
#' lubridate
#' rvest
#' xml2
#' stringr
#'
#' @export

festivo_plasencia <- function(){

  fecha <- Sys.Date()
  mes <- month(fecha)
  dia <- day(fecha)

  if(mes < 10){
    num <- paste("0",mes,sep = "")
  }else{
    num <- as.character(i)
  }

  url <- "https://calendarios.ideal.es/laboral/extremadura/caceres/plasencia"
  html_inicial <- url %>% GET(., timeout(30)) %>% read_html()

  festivos <- c()
  selector_xpath <- paste("//table[contains(@class, 'bm-calendar bm-calendar-month-",num," bm-calendar-year-2023')]//td[contains(@class, 'bm-calendar-state-nacional')]",sep = "")
  if(!identical(sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text),list())){
    festivos <- c(festivos, sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text))
  }
  selector_xpath <- paste("//table[contains(@class, 'bm-calendar bm-calendar-month-",num," bm-calendar-year-2023')]//td[contains(@class, 'bm-calendar-state-autonomico')]",sep = "")
  if(!identical(sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text),list())){
    festivos <- c(festivos, sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text))
  }
  selector_xpath <- paste("//table[contains(@class, 'bm-calendar bm-calendar-month-",num," bm-calendar-year-2023')]//td[contains(@class, 'bm-calendar-state-local')]",sep = "")
  if(!identical(sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text),list())){
    festivos <- c(festivos, sapply(xml_find_all(html_inicial, xpath = selector_xpath), xml_text))
  }

  if(!is.na(match(dia,as.numeric(festivos)))){
    festivo <- 1
  }else{
    festivo <- 0
  }


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
  # GUARDADO EN PLATAFORMA
  # ------------------------------------------------------------------------------

  id_activo <- "07c323a0-43ee-11ed-b077-bb6dc81b6e02"
  url <- paste("https://plataforma.plasencia.es/api/plugins/telemetry/ASSET/", id_activo, "/SERVER_SCOPE",sep = "")
  json_envio_plataforma <- paste('{"festivo":', festivo,
                                 '}',sep = "")

  post <- httr::POST(url = url,
                     add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                     body = json_envio_plataforma,
                     verify= FALSE,
                     encode = "json",verbose()
  )

}
