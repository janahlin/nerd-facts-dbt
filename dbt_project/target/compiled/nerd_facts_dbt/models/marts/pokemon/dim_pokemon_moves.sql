

/*
  Model: dim_pokemon_moves
  Description: Dimension table for Pokémon moves
*/

WITH base_moves AS (
    -- Ensure clean numeric data from source
    SELECT
        move_id,
        move_name,
        type->>'name' AS move_type,
        -- Handle numeric conversions safely
        CASE 
            WHEN power::TEXT ~ '^[0-9]+$' THEN power::INTEGER
            ELSE NULL 
        END AS power,
        CASE 
            WHEN pp::TEXT ~ '^[0-9]+$' THEN pp::INTEGER
            ELSE NULL 
        END AS pp,
        CASE 
            WHEN accuracy::TEXT ~ '^[0-9]+$' THEN accuracy::INTEGER
            ELSE NULL 
        END AS accuracy,
        CASE 
            WHEN priority::TEXT ~ '^[0-9]+$' THEN priority::INTEGER
            ELSE 0 
        END AS priority,
        damage_class->>'name' AS damage_class,
        jsonb_array_elements(effect_entries::jsonb)->>'effect' AS effect_text,
        CASE 
            WHEN effect_chance::TEXT ~ '^[0-9]+$' THEN effect_chance::INTEGER
            ELSE NULL 
        END AS effect_chance,
        CASE 
            WHEN (generation->>'url')::TEXT ~ 'generation/([0-9]+)/' 
            THEN REGEXP_REPLACE((generation->>'url')::TEXT, '.*generation/([0-9]+)/.*', '\1')::INTEGER
            ELSE 1 
        END AS generation_id
    FROM "nerd_facts"."public"."stg_pokeapi_moves"
    WHERE move_id IS NOT NULL
),

move_classifications AS (
    -- Pre-calculate move classifications to avoid repetition
    SELECT
        move_id,
        move_name,
        move_type,
        power,
        pp,
        accuracy,
        priority,
        damage_class,
        effect_text,
        effect_chance,
        generation_id,
        -- Add effect category calculation
        CASE
            WHEN effect_text LIKE '%boost%' OR 
                 effect_text LIKE '%raise%' OR 
                 effect_text LIKE '%increase%' THEN 'Stat Boost'
            WHEN effect_text LIKE '%lower%' OR 
                 effect_text LIKE '%decrease%' OR 
                 effect_text LIKE '%reduce%' THEN 'Stat Reduction'
            WHEN effect_text LIKE '%paralyze%' THEN 'Paralyze'
            WHEN effect_text LIKE '%burn%' THEN 'Burn'
            WHEN effect_text LIKE '%poison%' OR 
                 effect_text LIKE '%toxic%' THEN 'Poison'
            WHEN effect_text LIKE '%sleep%' THEN 'Sleep'
            WHEN effect_text LIKE '%freeze%' THEN 'Freeze'
            WHEN effect_text LIKE '%confus%' THEN 'Confusion'
            WHEN effect_text LIKE '%flinch%' THEN 'Flinch'
            WHEN effect_text LIKE '%trap%' THEN 'Trap'
            WHEN effect_text LIKE '%heal%' OR 
                 effect_text LIKE '%restore%' THEN 'Healing'
            WHEN effect_text LIKE '%protect%' OR 
                 effect_text LIKE '%protect%' THEN 'Protection'
            WHEN effect_text LIKE '%critical%' THEN 'Critical Hit'
            WHEN effect_text LIKE '%priority%' OR priority > 0 THEN 'Priority'
            WHEN damage_class = 'status' THEN 'Status Effect'
            WHEN power > 0 THEN 'Direct Damage'
            ELSE 'Other'
        END AS effect_category
    FROM base_moves
)

SELECT
    -- Primary key
    md5(cast(coalesce(cast(move_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS move_key,
    
    -- Core identifiers
    move_id,
    move_name,
    
    -- Move attributes
    move_type,
    COALESCE(power, 0) AS power,
    COALESCE(pp, 0) AS pp,
    COALESCE(accuracy, 0) AS accuracy,
    COALESCE(priority, 0) AS priority,
    COALESCE(damage_class, 'physical') AS damage_class,
    
    -- Effect information
    effect_text,
    COALESCE(effect_chance, 0) AS effect_chance,
    
    -- Generation information
    COALESCE(generation_id, 1) AS generation_id,
    
    -- Move category classification
    CASE
        WHEN damage_class = 'status' THEN 'Status'
        WHEN power IS NULL OR power = 0 THEN 'Status'
        WHEN power < 40 THEN 'Weak'
        WHEN power < 70 THEN 'Medium'
        WHEN power < 90 THEN 'Strong'
        WHEN power < 110 THEN 'Very Strong'
        ELSE 'Extreme'
    END AS power_category,
    
    -- Accuracy classification
    CASE
        WHEN accuracy IS NULL THEN 'Always Hits'
        WHEN accuracy < 50 THEN 'Very Inaccurate'
        WHEN accuracy < 70 THEN 'Inaccurate'
        WHEN accuracy < 85 THEN 'Moderate'
        WHEN accuracy < 95 THEN 'Accurate'
        WHEN accuracy < 100 THEN 'Very Accurate'
        WHEN accuracy = 100 THEN 'Perfect'
        ELSE 'Unknown'
    END AS accuracy_category,
    
    -- Move effect category based on effect text
    effect_category,
    
    -- Competitive relevance score (1-10)
    CASE
        -- Extremely useful moves in competitive
        WHEN move_name IN ('stealth-rock', 'spikes', 'toxic-spikes', 'defog', 'rapid-spin',
                   'recover', 'wish', 'protect', 'substitute', 'will-o-wisp',
                   'scald', 'knock-off', 'u-turn', 'volt-switch', 'toxic',
                   'thunder-wave', 'dragon-dance', 'swords-dance', 'nasty-plot',
                   'calm-mind', 'quiver-dance', 'roost', 'leech-seed') THEN 10
                   
        -- Very strong attacks and utility moves
        WHEN (power > 100 AND accuracy >= 90) OR 
             move_name IN ('close-combat', 'earthquake', 'ice-beam', 'thunderbolt',
                   'flamethrower', 'surf', 'stone-edge', 'focus-blast',
                   'shadow-ball', 'psyshock', 'earth-power', 'draco-meteor', 
                   'moonblast', 'play-rough', 'gunk-shot', 'brave-bird') THEN 9
                   
        -- Strong utility and common attacks
        WHEN (power >= 80 AND accuracy >= 85) OR
             move_name IN ('taunt', 'encore', 'trick', 'toxic', 'synthesis', 
                   'aromatherapy', 'heal-bell', 'sticky-web', 'trick-room',
                   'tailwind', 'moonlight', 'morning-sun') THEN 8
                   
        -- Useful moves but not top tier
        WHEN (power >= 70 AND accuracy >= 80) OR
             move_name IN ('light-screen', 'reflect', 'hypnosis', 'sleep-powder',
                   'stun-spore', 'thunder-wave') THEN 7
                   
        -- Standard damage moves with decent stats
        WHEN power >= 60 AND accuracy >= 90 THEN 6
        
        -- Status moves without specific utility
        WHEN damage_class = 'status' THEN 5
        
        -- Weak but accurate moves
        WHEN power < 60 AND accuracy > 90 THEN 4
        
        -- Inaccurate moves
        WHEN accuracy < 80 AND power > 0 THEN 3
        
        -- Very weak moves
        WHEN power < 40 THEN 2
        
        -- Other moves
        ELSE 1
    END AS competitive_score,
    
    -- Move uniqueness rating
    CASE
        -- Signature moves
        WHEN move_name IN ('spacial-rend', 'roar-of-time', 'seed-flare', 'blue-flare',
                         'bolt-strike', 'fusion-flare', 'fusion-bolt', 'origin-pulse',
                         'precipice-blades', 'dragon-ascent', 'sacred-fire', 'aeroblast',
                         'shadow-force', 'doom-desire', 'psycho-boost', 'lunar-dance',
                         'magma-storm', 'crush-grip', 'judgment', 'secret-sword',
                         'relic-song', 'light-of-ruin', 'steam-eruption', 'core-enforcer',
                         'sunsteel-strike', 'moongeist-beam', 'photon-geyser',
                         'spectral-thief', 'plasma-fists') THEN 'Signature'
        -- Very rare moves (limited distribution)
        WHEN move_name IN ('shell-smash', 'quiver-dance', 'tail-glow', 'dragon-dance',
                   'shift-gear', 'coil', 'geomancy', 'mind-blown', 'oblivion-wing',
                   'thousand-arrows', 'thousand-waves', 'diamond-storm') THEN 'Very Rare'
                   
        -- Rare but distributed moves
        WHEN move_name IN ('spore', 'dark-void', 'healing-wish', 'lunar-dance', 
                   'shore-up', 'belly-drum', 'fiery-dance', E'king''s-shield',
                   'spiky-shield', 'baneful-bunker', 'parting-shot') THEN 'Rare'
                   
        -- Uncommon moves
        WHEN move_name IN ('leech-seed', 'aromatherapy', 'heal-bell', 'sticky-web',
                   'defog', 'rapid-spin', 'extreme-speed', 'sucker-punch',
                   'bullet-punch', 'aqua-jet', 'mach-punch', 'ice-shard') THEN 'Uncommon'
                   
        -- Common coverage moves
        WHEN move_name IN ('ice-beam', 'thunderbolt', 'flamethrower', 'surf',
                   'earthquake', 'stone-edge', 'close-combat', 'shadow-ball') THEN 'Standard'
                   
        -- Very common moves
        WHEN move_name IN ('toxic', 'protect', 'rest', 'sleep-talk', 'substitute') THEN 'Common'
                   
        -- Handle everything else based on damage class
        WHEN damage_class = 'status' THEN 'Status'
        ELSE 'Standard'
    END AS move_rarity,
    
    -- Data tracking field
    CURRENT_TIMESTAMP AS dbt_loaded_at
    
FROM move_classifications
ORDER BY move_type, power DESC NULLS LAST, move_name