PUT file://C:/Users/Usuario/Documents/Andres_Castro/prubas_tecnicas/prueba_rappy_test/data/topics.csv @RAW_DATA.STG_MEETUP AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://C:/Users/Usuario/Documents/Andres_Castro/prubas_tecnicas/prueba_rappy_test/data/venues.csv @RAW_DATA.STG_MEETUP AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://C:/Users/Usuario/Documents/Andres_Castro/prubas_tecnicas/prueba_rappy_test/data/members_topics.csv @RAW_DATA.STG_MEETUP AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8;
PUT file://C:/Users/Usuario/Documents/Andres_Castro/prubas_tecnicas/prueba_rappy_test/data/members.csv @RAW_DATA.STG_MEETUP AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8;

LIST @RAW_DATA.STG_MEETUP;