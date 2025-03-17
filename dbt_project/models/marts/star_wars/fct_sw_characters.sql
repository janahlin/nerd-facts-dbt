{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['character_id']}, {'columns': ['species_id']}, {'columns': ['homeworld_id']}, {'columns': ['character_tier']}],
    unique_key = 'character_key'
  )
}}

/*
  Model: fct_sw_characters
  Description: Fact table for Star Wars characters with comprehensive attributes and metrics
  
  Notes:
  - Contains detailed character information and derived attributes
  - Provides character classifications by role, affiliation, and significance
  - Calculates Force sensitivity and combat effectiveness ratings
  - Enriches character context with Star Wars universe specifics
  - Serves as the primary analytical table for character analysis
  - Enhanced with additional fields from updated staging models
*/

WITH base_characters AS (
    SELECT
        id AS character_id,
        name,
        NULLIF(height, 'unknown')::NUMERIC AS height,
        NULLIF(mass, 'unknown')::NUMERIC AS mass,
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        homeworld AS homeworld_id,
        species_id,
        
        -- Use enhanced fields from staging
        film_appearances,
        film_names,
        vehicle_count, -- Use pre-calculated count
        vehicle_names,
        starship_count, -- Use pre-calculated count
        starship_names,
        force_sensitive, -- Use this flag directly
        character_era,   -- Use this field for era classification
        
        -- Add source tracking
        url,
        fetch_timestamp,
        processed_timestamp
    FROM {{ ref('stg_swapi_people') }}
    WHERE id IS NOT NULL
),

-- Join with species information
character_species AS (
    SELECT
        c.*,
        s.name AS species_name,
        s.classification AS species_classification,
        s.average_lifespan,
        s.language
    FROM base_characters c
    LEFT JOIN {{ ref('stg_swapi_species') }} s ON c.species_id = s.id
),

-- Join with homeworld information
character_homeworld AS (
    SELECT
        cs.*,
        p.planet_name AS homeworld_name,
        p.terrain AS homeworld_terrain,
        p.climate AS homeworld_climate
    FROM character_species cs
    LEFT JOIN {{ ref('stg_swapi_planets') }} p ON cs.homeworld_id = p.id
),

-- Calculate force sensitivity and ratings with expanded logic
force_ratings AS (
    SELECT
        character_id,
        -- Use force_sensitive from staging when available
        COALESCE(force_sensitive, 
            CASE
                WHEN LOWER(name) IN (
                    'luke skywalker', 'darth vader', 'leia organa', 'yoda', 'emperor palpatine', 
                    'obi-wan kenobi', 'qui-gon jinn', 'mace windu', 'count dooku', 'darth maul',
                    'rey', 'kylo ren', 'anakin skywalker', 'ahsoka tano', 'plo koon',
                    'kit fisto', 'aayla secura', 'luminara unduli', 'barriss offee',
                    'asajj ventress', 'shaak ti', 'ki-adi-mundi', 'yaddle', 'ezra bridger',
                    'kanan jarrus', 'grogu', 'ben solo', 'sheev palpatine', 'snoke'
                ) THEN TRUE
                ELSE FALSE
            END
        ) AS force_sensitive,
        
        -- Force rating on 1-10 scale with more comprehensive categorization
        CASE
            -- Legendary Force users
            WHEN LOWER(name) IN ('yoda', 'emperor palpatine', 'darth vader', 'luke skywalker') THEN 10
            
            -- Very powerful Force users
            WHEN LOWER(name) IN ('mace windu', 'count dooku', 'rey', 'kylo ren') THEN 9
            
            -- Powerful Jedi Masters / Sith Lords
            WHEN LOWER(name) IN ('obi-wan kenobi', 'qui-gon jinn', 'darth maul') THEN 8
            
            -- Advanced Jedi / Dark Side users
            WHEN LOWER(name) IN ('ahsoka tano', 'kit fisto', 'plo koon', 'ki-adi-mundi') THEN 7
            
            -- Trained Jedi / Force users
            WHEN LOWER(name) IN ('luminara unduli', 'barriss offee', 'aayla secura') THEN 6
            
            -- Powerful but untrained
            WHEN LOWER(name) IN ('grogu', 'leia organa') THEN 5
            
            -- All other known Force sensitives
            WHEN LOWER(name) IN (
                'ezra bridger', 'kanan jarrus', 'yaddle', 'shaak ti', 'asajj ventress'
            ) THEN 4
            
            -- Default for force sensitives not explicitly rated
            WHEN force_sensitive THEN 3
            
            -- Non-Force sensitive
            ELSE 0
        END AS force_rating,
        
        -- Force alignment
        CASE
            -- Light side users
            WHEN LOWER(name) IN (
                'luke skywalker', 'yoda', 'obi-wan kenobi', 'qui-gon jinn', 'mace windu',
                'ahsoka tano', 'plo koon', 'kit fisto', 'aayla secura', 'luminara unduli',
                'barriss offee', 'shaak ti', 'ki-adi-mundi', 'yaddle', 'kanan jarrus'
            ) THEN 'Light Side'
            
            -- Dark side users
            WHEN LOWER(name) IN (
                'darth vader', 'emperor palpatine', 'count dooku', 'darth maul',
                'kylo ren', 'asajj ventress', 'snoke'
            ) THEN 'Dark Side'
            
            -- Characters who walked both paths
            WHEN LOWER(name) IN ('rey', 'anakin skywalker', 'ben solo') THEN 'Both Light/Dark'
            
            -- Force sensitive but neutral/unknown alignment
            WHEN force_sensitive THEN 'Neutral/Unknown'
            
            -- Not Force sensitive
            ELSE 'Not Force Sensitive'
        END AS force_alignment
    FROM base_characters
),

-- Estimate combat experience and abilities with expanded classifications
battle_experience AS (
    SELECT
        ch.character_id,
        ch.name,
        ch.film_appearances AS film_count, -- Use the field from staging
        ch.vehicle_count AS vehicles_count, -- Use the field from staging
        ch.starship_count AS starships_count, -- Use the field from staging 
        ch.film_names, -- Add film names array
        ch.vehicle_names, -- Add vehicle names array
        ch.starship_names, -- Add starship names array
        ch.force_sensitive, -- Use the field from staging
        ch.character_era, -- Use the field from staging
        fr.force_rating,
        ch.url, -- Add source URL
        ch.fetch_timestamp, -- Add fetch timestamp
        ch.processed_timestamp, -- Add processed timestamp
        
        -- Enhanced battles won with more character context
        CASE
            -- Major heroes with known victories (higher counts)
            WHEN LOWER(ch.name) IN ('luke skywalker', 'leia organa', 'han solo', 'darth vader') THEN 
                10 + ch.film_appearances + ch.starship_count
                
            -- Important fighters and warriors
            WHEN LOWER(ch.name) IN ('boba fett', 'jango fett', 'chewbacca', 'lando calrissian',
                               'finn', 'poe dameron', 'rey', 'kylo ren', 'cassian andor') THEN 
                8 + ch.film_appearances
                
            -- Established Jedi and Sith
            WHEN LOWER(ch.name) IN ('obi-wan kenobi', 'qui-gon jinn', 'mace windu', 
                               'darth maul', 'count dooku', 'yoda', 'emperor palpatine') THEN 
                7 + fr.force_rating
                
            -- Military/combat characters
            WHEN LOWER(ch.name) LIKE '%captain%' OR 
                 LOWER(ch.name) LIKE '%commander%' OR 
                 LOWER(ch.name) LIKE '%general%' THEN 6
                 
            -- Characters who likely have combat experience
            WHEN ch.starship_count > 0 THEN 3 + ch.starship_count -- Pilots typically see combat
            WHEN ch.vehicle_count > 0 THEN 2 + ch.vehicle_count -- Vehicle operators likely have some combat
            WHEN ch.force_sensitive THEN 3 + fr.force_rating -- Force users typically engage in combat
            
            -- Everyone else gets minimal combat experience based on films
            ELSE GREATEST(1, ch.film_appearances)
        END AS battles_won,
        
        -- Derive more detailed combat role
        CASE
            -- Force users by type
            WHEN fr.force_alignment = 'Light Side' AND fr.force_rating >= 7 THEN 'Jedi Master'
            WHEN fr.force_alignment = 'Light Side' AND fr.force_rating >= 3 THEN 'Jedi Knight'
            WHEN fr.force_alignment = 'Dark Side' AND fr.force_rating >= 7 THEN 'Sith Lord'
            WHEN fr.force_alignment = 'Dark Side' AND fr.force_rating >= 3 THEN 'Dark Side User'
            
            -- Specific roles based on character knowledge
            WHEN LOWER(ch.name) IN ('han solo', 'lando calrissian', 'poe dameron') THEN 'Ace Pilot'
            WHEN LOWER(ch.name) IN ('boba fett', 'jango fett', 'cad bane') THEN 'Bounty Hunter'
            WHEN LOWER(ch.name) IN ('padmé amidala', 'mon mothma', 'bail organa') THEN 'Political Leader'
            WHEN LOWER(ch.name) IN ('c-3po', 'r2-d2', 'bb-8') THEN 'Droid'
            WHEN LOWER(ch.name) IN ('general grievous') THEN 'Cyborg Commander'
            
            -- Derive roles from attributes
            WHEN LOWER(ch.name) LIKE '%general%' OR LOWER(ch.name) LIKE '%admiral%' THEN 'Military Commander'
            WHEN LOWER(ch.name) LIKE '%captain%' OR LOWER(ch.name) LIKE '%commander%' THEN 'Military Officer'
            
            -- Role based on vehicles/starships
            WHEN ch.starship_count > 1 THEN 'Pilot'
            WHEN ch.vehicle_count > 1 THEN 'Vehicle Operator'
            WHEN ch.force_sensitive THEN 'Force Sensitive'
            ELSE 'Support/Other'
        END AS combat_role,
        
        -- Battle effectiveness on 1-10 scale
        CASE
            -- Legendary warriors
            WHEN LOWER(ch.name) IN ('darth vader', 'yoda', 'luke skywalker', 'emperor palpatine') THEN 10
            
            -- Elite fighters
            WHEN LOWER(ch.name) IN ('boba fett', 'jango fett', 'mace windu', 'general grievous',
                               'obi-wan kenobi', 'count dooku', 'darth maul') THEN 9
                               
            -- Accomplished fighters
            WHEN LOWER(ch.name) IN ('han solo', 'leia organa', 'kylo ren', 'rey', 'poe dameron', 
                               'finn', 'qui-gon jinn', 'chewbacca') THEN 8
                               
            -- Skilled combatants
            WHEN LOWER(ch.name) IN ('lando calrissian', 'padmé amidala', 'cassian andor',
                               'jyn erso', 'din djarin', 'cara dune', 'ahsoka tano') THEN 7
                               
            -- Force users get a base effectiveness plus their force rating
            WHEN ch.force_sensitive THEN LEAST(9, 4 + (fr.force_rating / 2))
            
            -- Pilots get decent effectiveness
            WHEN ch.starship_count > 0 THEN LEAST(7, 4 + ch.starship_count)
            
            -- Everyone else scaled by films and vehicles
            ELSE GREATEST(2, LEAST(6, 2 + ch.film_appearances + ch.vehicle_count))
        END AS battle_effectiveness
    FROM character_homeworld ch
    JOIN force_ratings fr ON ch.character_id = fr.character_id
)

SELECT
    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['be.character_id']) }} AS character_key,
    
    -- Core identifiers
    be.character_id,
    be.name AS character_name,
    
    -- Dimension references with proper keys
    be.homeworld_id,
    ch.homeworld_name,
    be.species_id,
    ch.species_name,
    
    -- Physical attributes with better formatting
    CASE WHEN ch.height IS NOT NULL THEN ch.height ELSE NULL END AS height_cm,
    CASE 
        WHEN ch.height IS NOT NULL 
        THEN FLOOR(ch.height / 100) || 'm ' || MOD(FLOOR(ch.height), 100) || 'cm'
        ELSE 'Unknown'
    END AS height_formatted,
    
    CASE WHEN ch.mass IS NOT NULL THEN ch.mass ELSE NULL END AS mass_kg,
    CASE 
        WHEN ch.mass IS NOT NULL 
        THEN ch.mass || ' kg'
        ELSE 'Unknown'
    END AS mass_formatted,
    
    -- Improved BMI calculation with better error handling
    CASE 
        WHEN ch.height IS NOT NULL AND ch.mass IS NOT NULL AND ch.height > 0 
        THEN ROUND(ch.mass / POWER(ch.height/100, 2), 1)
        ELSE NULL
    END AS bmi,
    
    -- Character classification
    CASE 
        WHEN ch.gender IS NOT NULL THEN ch.gender 
        WHEN LOWER(ch.species_name) LIKE '%droid%' THEN 'Droid' 
        ELSE 'Unknown'
    END AS gender,
    
    ch.birth_year,
    ch.hair_color,
    ch.eye_color,
    ch.skin_color,
    
    -- Species attributes
    ch.species_classification,
    ch.average_lifespan,
    ch.language,
    
    -- Homeworld attributes
    ch.homeworld_climate,
    ch.homeworld_terrain,
    
    -- Force powers and alignment
    be.force_sensitive,
    be.force_rating,
    fr.force_alignment,
    
    -- Combat metrics
    be.combat_role,
    be.battles_won,
    be.battle_effectiveness,
    
    -- Vehicle, starship, and film stats
    be.starships_count,
    be.vehicles_count,
    be.film_count,
    
    -- Vehicle, starship, and film name arrays for detailed reporting
    be.film_names AS film_appearances_list,
    be.vehicle_names AS vehicle_list,
    be.starship_names AS starship_list,
    
    -- Character importance and affiliation
    -- Character affiliation with extensive mapping
    CASE
        -- Rebel/Resistance affiliated
        WHEN LOWER(be.name) IN ('luke skywalker', 'leia organa', 'han solo', 'chewbacca',
                            'lando calrissian', 'mon mothma', 'admiral ackbar',
                            'wedge antilles', 'poe dameron', 'finn', 'rey') THEN 'Rebel Alliance/Resistance'
        
        -- Empire/First Order affiliated
        WHEN LOWER(be.name) IN ('darth vader', 'emperor palpatine', 'grand moff tarkin', 
                            'general hux', 'captain phasma', 'kylo ren', 'director krennic',
                            'moff gideon') THEN 'Empire/First Order'
        
        -- Republic affiliated
        WHEN LOWER(be.name) IN ('padmé amidala', 'bail organa', 'captain panaka',
                            'jar jar binks', 'clone troopers') THEN 'Galactic Republic'
        
        -- Jedi Order
        WHEN LOWER(be.name) IN ('yoda', 'mace windu', 'qui-gon jinn', 'obi-wan kenobi',
                            'ki-adi-mundi', 'plo koon', 'kit fisto', 'shaak ti',
                            'luminara unduli', 'barriss offee', 'aayla secura') THEN 'Jedi Order'
        
        -- Sith/Dark Side
        WHEN LOWER(be.name) IN ('count dooku', 'darth maul', 'asajj ventress', 'savage opress', 'snoke') THEN 'Sith/Dark Side'
        
        -- Criminal/Underworld
        WHEN LOWER(be.name) IN ('jabba the hutt', 'boba fett', 'jango fett', 'greedo', 'dengar',
                            'bossk', 'zam wesell', 'aurra sing') THEN 'Criminal/Bounty Hunter'
        
        -- Neutral/Independent
        WHEN LOWER(be.name) IN ('maz kanata', 'unkar plutt', 'watto', 'dex', 'dr. evazan') THEN 'Neutral/Independent'
        
        -- Separatist
        WHEN LOWER(be.name) IN ('general grievous', 'nute gunray', 'wat tambor', 'poggle the lesser') THEN 'Separatist Alliance'
        
        -- Droids typically follow their masters
        WHEN LOWER(be.name) IN ('r2-d2', 'c-3po', 'bb-8') THEN 'Rebel Alliance/Resistance'
        WHEN LOWER(be.name) IN ('battle droids', 'super battle droids') THEN 'Separatist Alliance'
        
        ELSE 'Unaffiliated/Unknown'
    END AS character_affiliation,
    
    -- Character significance tier
    CASE
        -- Main protagonists/antagonists
        WHEN LOWER(be.name) IN ('luke skywalker', 'darth vader', 'leia organa', 'han solo',
                            'rey', 'kylo ren', 'anakin skywalker', 'obi-wan kenobi',
                            'emperor palpatine', 'yoda') THEN 'S'
        
        -- Major supporting characters
        WHEN LOWER(be.name) IN ('chewbacca', 'r2-d2', 'c-3po', 'lando calrissian',
                            'padmé amidala', 'qui-gon jinn', 'mace windu', 'bb-8',
                            'poe dameron', 'finn', 'count dooku', 'darth maul', 
                            'boba fett', 'jango fett', 'grand moff tarkin') THEN 'A'
        
        -- Important secondary characters
        WHEN LOWER(be.name) IN ('admiral ackbar', 'mon mothma', 'wedge antilles',
                            'general grievous', 'jabba the hutt', 'watto',
                            'ki-adi-mundi', 'bail organa', 'jar jar binks',
                            'captain phasma', 'general hux', 'greedo', 
                            'uncle owen', 'aunt beru') THEN 'B'
        
        -- Minor characters with multiple appearances
        WHEN be.film_count > 1 THEN 'C'
        
        -- One-off characters
        ELSE 'D'
    END AS character_tier,
    
    -- Era appearance classification - use character_era from staging when available
    COALESCE(be.character_era, 
        CASE
            WHEN LOWER(be.name) IN ('qui-gon jinn', 'darth maul', 'watto', 'jar jar binks',
                                'young anakin', 'shmi skywalker', 'padmé amidala',
                                'nute gunray', 'captain panaka', 'sebulba', 'kitster',
                                'ric olié', 'boss nass', 'rune haako') THEN 'Prequel Era Only'
                                
            WHEN LOWER(be.name) IN ('count dooku', 'jango fett', 'zam wesell', 'dexter jettster',
                                'bail organa', 'cliegg lars', 'san hill', 'taun we', 'wat tambor',
                                'shaak ti', 'barriss offee', 'clone troopers') THEN 'Prequel Era Only'
                                
            WHEN LOWER(be.name) IN ('grand moff tarkin', 'jabba the hutt', 'greedo', 'biggs darklighter',
                                'wedge antilles', 'admiral ackbar', 'mon mothma', 'wicket w. warrick',
                                'admiral piett', 'general veers', 'boba fett', 'bossk',
                                'lobot') THEN 'Original Trilogy Only'
                                
            WHEN LOWER(be.name) IN ('rey', 'finn', 'poe dameron', 'bb-8', 'kylo ren',
                                'general hux', 'supreme leader snoke', 'captain phasma',
                                'maz kanata', 'lor san tekka', 'unkar plutt') THEN 'Sequel Era Only'
                                
            WHEN LOWER(be.name) IN ('luke skywalker', 'leia organa', 'han solo', 'chewbacca',
                                'r2-d2', 'c-3po', 'darth vader', 'emperor palpatine',
                                'yoda', 'obi-wan kenobi', 'lando calrissian') THEN 'Multiple Eras'
                                
            ELSE 'Unknown Era'
        END
    ) AS era_appearance,
    
    -- Popularity score on 1-100 scale
    GREATEST(1, LEAST(100, 
        CASE
            -- S-tier characters
            WHEN LOWER(be.name) IN ('luke skywalker', 'darth vader') THEN 95
            WHEN LOWER(be.name) IN ('leia organa', 'han solo', 'yoda') THEN 90
            WHEN LOWER(be.name) IN ('r2-d2', 'c-3po', 'chewbacca', 'obi-wan kenobi') THEN 85
            
            -- A-tier characters
            WHEN LOWER(be.name) IN ('boba fett', 'emperor palpatine', 'lando calrissian') THEN 80
            WHEN LOWER(be.name) IN ('rey', 'kylo ren', 'bb-8', 'qui-gon jinn') THEN 75
            
            -- B-tier characters
            WHEN LOWER(be.name) IN ('mace windu', 'padmé amidala', 'anakin skywalker') THEN 70
            WHEN LOWER(be.name) IN ('count dooku', 'darth maul', 'jabba the hutt', 'jango fett') THEN 65
            
            -- Based on appearances and role
            ELSE (
                (COALESCE(be.film_count, 0) * 10) +  -- Films are important
                (CASE WHEN be.force_sensitive THEN 15 ELSE 0 END) +  -- Force users are popular
                (CASE WHEN be.starships_count > 0 THEN 10 ELSE 0 END) +  -- Pilots are cool
                (CASE 
                    WHEN be.combat_role IN ('Jedi Master', 'Sith Lord', 'Bounty Hunter', 'Ace Pilot') THEN 15
                    WHEN be.combat_role IN ('Jedi Knight', 'Dark Side User', 'Military Commander') THEN 10
                    ELSE 0 
                END)
            )
        END
    )) AS popularity_score,
    
    -- Source data metadata
    be.url AS source_url,
    be.fetch_timestamp,
    be.processed_timestamp,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM battle_experience be
JOIN character_homeworld ch ON be.character_id = ch.character_id
JOIN force_ratings fr ON be.character_id = fr.character_id
ORDER BY character_tier, popularity_score DESC