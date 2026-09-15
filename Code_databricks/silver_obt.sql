CREATE OR REFRESH STREAMING TABLE silver_obt
AS 


    SELECT
        
            uber.bronze.stg_rides.*
                    
                    ,
                
        
            vehicle_make
                    
                    ,
                
        
            vehicle_type,description,base_rate,per_mile,per_minute
                    
                    ,
                
        
            payment_method,is_card,requires_auth
                    
                    ,
                
        
            ride_status,is_completed
                    
                    ,
                
        
            city,state,region,updated_at
                    
                    ,
                
        
            cancellation_reason
        


    FROM 
        
            
                STREAM uber.bronze.stg_rides
                WATERMARK booking_timestamp delay OF INTERVAL 3 MINUTES
            
            
        
                LEFT JOIN uber.bronze.map_vehicle_makes ON uber.bronze.stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id 
            
        
            
                LEFT JOIN uber.bronze.map_vehicle_types ON uber.bronze.stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id 
            
        
            
                LEFT JOIN uber.bronze.map_payment_methods ON uber.bronze.stg_rides.payment_method_id = map_payment_methods.payment_method_id 
            
        
            
                LEFT JOIN uber.bronze.map_ride_statuses ON uber.bronze.stg_rides.ride_status_id = map_ride_statuses.ride_status_id 
            
        
            
                LEFT JOIN uber.bronze.map_cities ON uber.bronze.stg_rides.pickup_city_id = map_cities.city_id
            
        
            
                LEFT JOIN uber.bronze.map_cancellation_reasons ON uber.bronze.stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
        

    
        
            
                 
                
                
                
                    
                    
        
            
                
                
                    
                    
        
            
                
                
                    
                    
        
            
                
                
        

--     SELECT
        
--             uber.bronze.stg_rides.*
                    
--                     ,
                
        
--             vehicle_make
                    
--                     ,
                
        
--             vehicle_type,description,base_rate,per_mile,per_minute
                
        


--     FROM 
        
            
--                 STREAM uber.bronze.stg_rides
--                 WATERMARK booking_timestamp delay OF INTERVAL 3 MINUTES

            
            
        
            
--                 LEFT JOIN uber.bronze.map_vehicle_makes ON uber.bronze.stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id 
            
        
            
--                 LEFT JOIN uber.bronze.map_vehicle_types ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id 
