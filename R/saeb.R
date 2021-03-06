get_catalog_saeb <-
	function( data_name = "saeb" , output_dir , ... ){

		inep_portal <- "http://portal.inep.gov.br/microdados"

		w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )
		
		these_links <- grep( "(aneb|saeb)(.*)zip$" , w , value = TRUE , ignore.case = TRUE )

		saeb_years <- gsub( "[^0-9]" , "" , these_links )

		catalog <-
			data.frame(
				year = saeb_years ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				output_folder = paste0( output_dir , "/" , saeb_years ) ,
				full_url = these_links ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_saeb <-
	function( data_name = "saeb" , catalog , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			tables_before <- DBI::dbListTables( db )
			
			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , "output_folder" ] )

			# manually set the encoding of the unziped files so they don't break things.
			Encoding( unzipped_files ) <- 'latin1'

			# does the zipped file contain any more zipped files?
			other.zips <- unzipped_files[ grep( '\\.zip$' , tolower( unzipped_files ) ) ]
			
			# if so, go through them, unzip them, add them to the object `unzipped_files`
			for ( this_zip in other.zips ){
				
				# unzip each of those again to the temporary directory
				z.zip <- unzip( this_zip , exdir = catalog[ i , "output_folder" ] )
			
				# add filepaths to the `unzipped_files` object each time
				unzipped_files <- c( unzipped_files , z.zip )
			}

			# this file is corrupt and not needed
			if ( any( to.fix <- grepl( "INPUTS_SAS_SPSS/ALUNOS/~$PUT_SAS_QUIMICA_03ANO.SAS" , unzipped_files , fixed = TRUE ) ) ){
				
				# delete it from its location on the hard drives
				file.remove( unzipped_files[ to.fix ] )
				
				# remove it from `unzipped_files`
				unzipped_files <- unzipped_files[ !to.fix ]
			}

			# since text files (containing data) and sas files (containing importation instructions) need to align
			# any files where they do not need to be renamed.  diretor, docente, escola, turma are all examples
			# of imperfect filename matches.  align them in every case in `unzipped_files`
			if( any( to.fix <- grepl( "DIRETOR_|DOCENTE_|ESCOLA_|TURMA_" , unzipped_files ) ) ){
				
				# rename all files containing `diretor` to `diretores`
				file.rename( unzipped_files[ to.fix ] , gsub( "DIRETOR_" , "DIRETORES_" , unzipped_files[ to.fix ] , fixed = TRUE ) )
				
				# update the `unzipped_files` object (the character vector containing the file positions on the local disk)
				unzipped_files <- gsub( "DIRETOR_" , "DIRETORES_" , unzipped_files , fixed = TRUE )

				# same as above
				file.rename( unzipped_files[ to.fix ] , gsub( "DOCENTE_" , "DOCENTES_" , unzipped_files[ to.fix ] , fixed = TRUE ) )
				unzipped_files <- gsub( "DOCENTE_" , "DOCENTES_" , unzipped_files , fixed = TRUE )

				# same as same as above
				file.rename( unzipped_files[ to.fix ] , gsub( "ESCOLA_" , "ESCOLAS_" , unzipped_files[ to.fix ] , fixed = TRUE ) )
				unzipped_files <- gsub( "ESCOLA_" , "ESCOLAS_" , unzipped_files , fixed = TRUE )

				# (same as)^3 above
				file.rename( unzipped_files[ to.fix ] , gsub( "TURMA_" , "TURMAS_" , unzipped_files[ to.fix ] , fixed = TRUE ) )
				unzipped_files <- gsub( "TURMA_" , "TURMAS_" , unzipped_files , fixed = TRUE )

			}

					
			# identify all files ending with `.sas` and `.txt` and `.csv`
			sas.files <- unzipped_files[ grep( '\\.sas$' , tolower( unzipped_files ) ) ]
			text.files <- unzipped_files[ grep( '\\.txt$' , tolower( unzipped_files ) ) ]
			csv.files <- unzipped_files[ grep( '\\.csv$' , tolower( unzipped_files ) ) ]
			# store each of those into separate character vectors, subsets of `unzipped_files`

			# confirm each sas file matches a text file and vice versa
			stopifnot ( all( gsub( "\\.txt$" , "" , tolower( basename( text.files ) ) ) %in% gsub( "i[m|n]put_sas_(.*)\\.sas$" , "\\1" , tolower( basename( sas.files ) ) ) ) ) 

			Encoding( sas.files ) <- ''
			
			# loop through each available sas importation file..
			for ( this_sas in sas.files ){
				
				# write the file to the disk
				w <- readLines( this_sas )
				
				# remove all tab characters
				w <- gsub( '\t' , ' ' , w )
				
				# overwrite the file on the disk with the newly de-tabbed text
				writeLines( w , this_sas )
			}

			# loop through each available txt (data) file..
			for ( this.text in text.files ){

				# remove the `.txt` to determine the name of the current table
				table.name <- gsub( "\\.txt$" , "" , tolower( basename( this.text ) ) )

				# add the year
				tnwy <- paste0( table.name , "_" , catalog[ i , 'year' ] )
				
				# find the appropriate sas importation instructions to be used for the current table
				this.sas <- sas.files[ match( table.name , gsub( "i[m|n]put_sas_(.*)\\.sas$" , "\\1" , tolower( basename( sas.files ) ) ) ) ]
				
				# read the data file directly into an R data frame object
				x <- read_SAScii( this.text , this.sas )

				# convert column names to lowercase
				names( x ) <- tolower( names( x ) )
				
				# do not use monetdb reserved words
				for ( j in names( x )[ toupper( names( x ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( x )[ names( x ) == j ] <- paste0( j , "_" )
				
				# store the `x` data.frame object in MonetDBLite database as well
				DBI::dbWriteTable( db , tnwy , x )
				
				# save the current table in the year-specific folder on the local drive
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , table.name , ".rds" ) )

			}

			# loop through each available csv (also data) file..
			for ( this.csv in csv.files ){
			
				# remove the `.csv` to determine the name of the current table
				tnwy <- paste0( gsub( "\\.csv$" , "" , tolower( basename( this.csv ) ) ) , "_" , catalog[ i , 'year' ] )

				# specify the chunk size to read in
				chunk_size <- 250000

				# create a file connection to the current csv
				input <- file( this.csv , "r")

				# read in the first chunk
				headers <- 
					read.csv( 
						input , 
						sep = if( catalog[ i , 'year' ] >= 2013 ) "," else ";" , 
						dec = if( catalog[ i , 'year' ] >= 2013 ) "." else "," , 
						na.strings = if( catalog[ i , 'year' ] >= 2013 ) "" else "." , 
						nrows = chunk_size ,
						stringsAsFactors = FALSE
					)
				
				# convert column names to lowercase
				names( headers ) <- tolower( names( headers ) )
				
				# do not use monetdb reserved words
				for ( j in names( headers )[ toupper( names( headers ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( headers )[ names( headers ) == j ] <- paste0( j , "_" )

				cc <- sapply( headers , class )

				# initiate the current table
				DBI::dbWriteTable( db , tnwy , headers , overwrite = TRUE , row.names = FALSE )
				
				# so long as there are lines to read, add them to the current table
				tryCatch({
				   while (TRUE) {
					   part <- 
						read.csv(
							input , 
							header = FALSE ,
							nrows = chunk_size , 
							sep = if( catalog[ i , 'year' ] >= 2013 ) "," else ";" ,
							dec = if( catalog[ i , 'year' ] >= 2013 ) "." else "," ,
							na.strings = if( catalog[ i , 'year' ] >= 2013 ) "" else "." , 
							colClasses = cc
						)
						
						# coerce logical columns to character
						part[ sapply( part , class ) == 'logical' ] <- sapply( part[ sapply( part , class ) == 'logical' ] , as.character )
						
					   DBI::dbWriteTable( db , tnwy , part , append = TRUE , row.names = FALSE )
				   }
				   
				} , error = function(e) { if ( grepl( "no lines available" , conditionMessage( e ) ) ) TRUE else stop( conditionMessage( e ) ) }
				)
					
			}
			
			tables_after <- setdiff( tables_before , DBI::dbListTables( db ) )
			
			for( this_table in tables_after ) catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , this_table ) )[ 1 , 1 ] , na.rm = TRUE )
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		catalog

	}

