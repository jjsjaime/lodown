get_catalog_mtps <-
  function( data_name = "mtps" , output_dir , ... ){

    output_dir <- gsub( "\\\\", "/", output_dir )

    url_path <- "ftp://ftp.mtps.gov.br/pdet/microdados/"

    mtps_files <- recursive_ftp_scrape( url_path )

    catalog <-
      data.frame(
        full_url = mtps_files ,
        stringsAsFactors = FALSE
      )

    catalog$type <-
      ifelse( grepl( "layout|xls$|pdf$" , catalog$full_url , ignore.case = TRUE ) , "docs" ,
              ifelse( grepl( "RAIS" , catalog$full_url ) , "rais" ,
                      ifelse( grepl( "CAGED" , catalog$full_url ) , "caged" , NA ) )
      )

    catalog$subtype[ catalog$type == "caged" ] <- NA
    catalog$subtype[ catalog$type == "rais" ] <-
      ifelse( grepl( "estb" , catalog$full_url[ catalog$type == "rais" ] , ignore.case = TRUE ) , "estabelecimento" , "vinculo" )

    catalog$output_filename <- ifelse( grepl( "7z$|zip$" , catalog$full_url , ignore.case = TRUE ) , NA ,
                                     paste0( output_dir , "/" , basename( tolower( catalog$full_url ) ) ) )

    catalog$year <- NULL
    catalog$year [ catalog$type %in% c( "caged" , "rais" ) ] <- as.numeric( gsub( ".*/" , "" , dirname( catalog$full_url ) )[ catalog$type %in% c( "caged" , "rais" ) ] )

    catalog$month <- NULL
    catalog$month [ catalog$type %in% c( "caged" ) ] <- substr( gsub( ".*_|\\..*", "", basename( catalog$full_url ) ) , 1 , 2 ) [ catalog$type %in% c( "caged" ) ]
    catalog$month <- as.numeric( catalog$month )

    catalog$db_tablename <-
      ifelse( catalog$type %in% c("caged" , "rais") ,
              paste0(
                ifelse( catalog$type == "caged" , "caged" , "rais_" ) ,
                ifelse( !is.na( catalog$subtype ) , paste0( catalog$subtype, "_" ) , "" ) ,
                ifelse( catalog$type == "rais" , catalog$year , "" ) ) , NA )

    catalog$dbfolder <- ifelse( is.na( catalog$db_tablename ) , NA , paste0( output_dir , "/MonetDB" ) )

    catalog

  }



lodown_mtps <-
  function( data_name = "mtps" , catalog , path_to_7z = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){


      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

      if( !grepl( ".7z$|.zip$" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){

        file.copy( tf , catalog[ i , 'output_filename' ] )

      } else {

        db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

        # build the string to send to DOS
        dos.command <- paste0( '"' , normalizePath( path_to_7z ) , '" x "' , normalizePath( tf ) , '" -o"' , paste0( tempdir() , "\\unzipped" ) , '"' )

        system( dos.command , show.output.on.console = FALSE )

        this_data_file <- list.files( paste0( tempdir() , "\\unzipped" ) , full.names = TRUE )

        this_data_file <- grep( "\\.csv|\\.txt$", this_data_file, value = TRUE, ignore.case = TRUE )

        cleaned_data_file <- standardize_mtps( this_data_file )

        num_cols <- length( readLines( cleaned_data_file , 1 )[[1]] )

        file.remove( this_data_file )

        suppressMessages(
          DBI::dbWriteTable(
            db,
            catalog[ i , 'db_tablename' ],
            cleaned_data_file ,
            sep = ";" ,
            header = TRUE ,
            lower.case.names = TRUE ,
            #nrow.check = 50000 ,
            colClasses = rep( "character" , num_cols ) ,
            append = TRUE
          ) )

        file.remove( cleaned_data_file )

      }

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored\r\n\n" ) )

    }

    # disconnect from the current monet database
    DBI::dbDisconnect( db , shutdown = TRUE )

    catalog

  }

standardize_mtps <-
  function( infile ){

    tf_a <- tempfile()

    outcon <- file( tf_a , "w" )

    incon <- file( infile , "r" )

    # read file header
    file.header <- strsplit( readLines( incon , 1 , warn = FALSE ) , ";" )[ 1 ] [[ 1 ]]

    # convert file header to lowercase
    file.header <- tolower( file.header )

    # remove special characters
    file.header <- iconv( file.header , "" , "ASCII//TRANSLIT" )

    # remove trailing spaces
    file.header <- trimws( file.header , which = "both" )

    # add underscores after monetdb illegal names
    for ( j in file.header [ toupper( file.header ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) file.header[ file.header == j ] <- paste0( j , "_" )

    # change dots, spaces , hiphens and bars to underscore
    file.header <- gsub( "\\.|/|-" , "_" , file.header )
    file.header <- gsub( " " , "_" , file.header )

    # create a standard csv line
    first.line <- paste0( file.header , collapse = ";" )

    # rewrite first line
    writeLines( first.line , outcon )

    while( length( line <- readLines( incon , 50000 , warn = FALSE ) ) > 0 ) writeLines( gsub( "\\{" ,  "" , iconv( line , "" , "ASCII//TRANSLIT" , sub = " " ) ) , outcon )

    close( incon )

    close( outcon )

    tf_a
  }
