get_catalog_bsapuf <-
	function( data_name = "bsapuf" , output_dir , ... ){

		file_links <- NULL

		file_pages <- c( "Inpatient_Claims" , "BSA_DME_Line_Items_PUF" , "BSA_PDE_PUF" , "Hospice_Bene" , "Carrier_Line_Items" , "HHA_PUF" , "Outpatient_Proc" , "SNF_Bene_PUF", "Chronic_Conditions_PUF" , "IPBS_PUF" , "Prescription_Drug_Profiles" )

		bsapuf_page <- "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/BSAPUFS/index.html"

		all_links <- rvest::html_attr( rvest::html_nodes( xml2::read_html( bsapuf_page ) , "a" ) , "href" )

		file_pages <- grep( paste0( file_pages , collapse = "|" ) , all_links , value = TRUE )

		for ( this_page in file_pages ){

			all_links <- rvest::html_attr( rvest::html_nodes( xml2::read_html( paste0( "https://www.cms.gov/" , this_page ) ) , "a" ) , "href" )

			possible_links <- grep( "puf(.*)\\.zip" , all_links , ignore.case = TRUE , value = TRUE )
			
			definite_links <- possible_links[ !grepl( "_DUG" , basename( possible_links ) ) ]
			
			file_links <- c( file_links , definite_links )

		}

		file_links[ grep( "^/" , file_links ) ] <- paste0(  "https://www.cms.gov" , file_links[ grep( "^/" , file_links ) ] )
			
		table_listing <- gsub( "[0-9]+_|_PUF|\\.zip" , "" , basename( file_links ) , ignore.case = TRUE )
		
		table_listing <- gsub( "_[0-9]$" , "" , table_listing , ignore.case = TRUE )
		
		with_year <- tolower( paste0( table_listing , "_" , substr( basename( file_links ) , 1 , 4 ) ) )
	
		catalog <-
			data.frame(
				year = substr( basename( file_links ) , 1 , 4 ) ,
				full_url = file_links ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = with_year ,
				stringsAsFactors = FALSE
			)

		catalog$full_url <- gsub( "http://" , "https://" , catalog$full_url , fixed = TRUE )
			
		catalog

	}


lodown_bsapuf <-
	function( data_name = "bsapuf" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			unzipped_files <- unzipped_files[ !grepl( "\\.xlsx$" , unzipped_files , ignore.case = TRUE ) ]
			
			stopifnot( length( unzipped_files ) == 1 )
			
			DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , unzipped_files , lower.case.names = TRUE , append = TRUE , nrow.check = 250000 )

			# if this is the final catalog entry for the unique db_tablename, store the case counts
			if( i == max( which( catalog$db_tablename == catalog[ i , 'db_tablename' ] ) ) ){
			
				catalog[ catalog$db_tablename == catalog[ i , 'db_tablename' ] , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog[ i , 'db_tablename' ] ) )

			}
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		catalog

	}

