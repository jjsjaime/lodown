get_catalog_addhealth <-
	function( data_name = "addhealth" , output_dir , ... ){

	catalog <- get_catalog_icpsr( study_numbers = "21600" , bundle_preference = "rdata" )
	
	catalog$wave <- tolower( stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 1 ) ) ) )
	
	catalog$data_title <- tolower( stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 2 ) ) ) )
	
	catalog$unzip_folder <- paste0( output_dir , "/" , catalog$wave , "/" , catalog$data_title , "/" )

	catalog

}


lodown_addhealth <-
	function( data_name = "addhealth" , catalog , ... ){
	
		lodown_icpsr( data_name = data_name , catalog , ... )

		catalog

	}

