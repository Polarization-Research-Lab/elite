import re, ast

prompt = """
Evaluate the provided text, with JSON results
Variable: personal_attack (yes or no).
Look for attacks on a person, Democrats, or Republicans.  All other groups are excluded.Attacks question the character, integrity, intelligence, morality or patriotism of a person or a political party. Do not count attacks on companies or groups, foreigners (such as Vladamir Putin), or terrorist groups.  Exclude statements where someone responds to an attack, talks about someone attacking them, when an attack is on attackers, is a quoted attack, or where the attack is on foreign lobbyists or agents.  Attacks on someone's job performance do not count. If the text criticizes a policy or legislation it is not an attack. The attack must be explicit and overt.
Variable: personal_attack_target
If this is an attack, who was attacked.  
Variable: policy_attack
Only if the text was not a personal attack, did it constructively criticize a policy, legislation, or court decisions.  Constructive criticism objects to policy, legislation and other governmental decisions with negative words, but relies on facts and doesn't use emotional appeals or personal attacks.
Variable: attack_reasoning
A 40-ish word explanation of your reasoning for personal_attack and/or policy_attack.
Variable: is_bipartisanship
If the text discusses bipartisanship or working across the aisle.  Or if it mentions collaboration, cooperation, compromise or willingness to find common ground between Democrats and Republicans.
Variable: bipartisanship_reasoning
A 20-ish word explanation for is_bipartisanship
Variable: is_creditclaiming
If text takes credit for: creating or passing legislation, government spending or grants, or if it discusses a politician’s accomplishments.
Variable: creditclaiming_reasoning
A 20-ish word explanation for is_creditclaiming
Variable: policy
If the text contains discussion of a public policy.  Discussion of policy can include discussion of specific legislation or general discussion such as healthcare, education, environment, foreign policy, the economy, defense spending, national security, etc. Procedural statements that don't specify a specific policy area should not be classified as `policy`.
Also, here are a list of policy areas:
"Agriculture and Food": agricultural practices; agricultural prices and marketing; agricultural education; food assistance or nutrition programs; food industry, supply, and safety; aquaculture; horticulture and plants. 
"Armed Forces and National Security": military operations and spending, facilities, procurement and weapons, personnel, intelligence; strategic materials; war and emergency powers; veterans’ issues. 
"Civil Rights and Liberties, Minority Issues": discrimination on basis of race, ethnicity, age, sex, gender, health or disability; First Amendment rights; due process and equal protection; abortion rights; privacy. 
"Commerce": business investment, development, regulation; small business; consumer affairs; competition and restrictive trade practices; manufacturing, distribution, retail; marketing; intellectual property. 
"Crime and Law Enforcement": criminal offenses, investigation and prosecution, procedure and sentencing; corrections and imprisonment; juvenile crime; law enforcement administration. 
"Economics and Public Finance": budgetary matters such as appropriations, public debt, the budget process, government lending, government accounts and trust funds; monetary policy and inflation; economic development, performance
"Education": elementary, secondary, or higher education including special education and matters of academic performance, school administration, teaching, educational costs, and student aid.
"Emergency Management": emergency planning; response to civil disturbances, natural and other disasters, including fires; emergency communications; security preparedness.
"Energy": all sources and supplies of energy, including alternative energy sources, oil and gas, coal, nuclear power; efficiency and conservation; costs, prices, and revenues; electric power transmission; public utility matters.a
"Environmental Protection": regulation of pollution including from hazardous substances and radioactive releases; climate change and greenhouse gasses; environmental assessment and research; solid waste and recycling; ecology. 
"Families": child and family welfare, services, and relationships; marriage and family status; domestic violence and child abuse. 
"Finance and Financial Sector": U.S. banking and financial institutions regulation; consumer credit; bankruptcy and debt collection; financial services and investments; insurance; securities; real estate transactions; currency. 
"Foreign Trade and International Finance": competitiveness, trade barriers and adjustment assistance; foreign loans and international monetary system; international banking; trade agreements and negotiations; customs enforcement, tariffs, and trade restrictions; foreign investment. 
"Government Operations and Politics": government administration, including agency organization, contracting, facilities and property, information management and services; rulemaking and administrative law; elections and political activities; government employees and officials; Presidents; ethics and public participation; postal service. 
"Health": science or practice of the diagnosis, treatment, and prevention of disease; health services administration and funding, including such programs as Medicare and Medicaid; health personnel and medical education; drug use and safety; health care coverage and insurance; health facilities. 
"Housing and Community Development": home ownership; housing programs administration and funding; residential rehabilitation; regional planning, rural and urban development; affordable housing; homelessness; housing industry and construction; fair housing. 
"Immigration": administration of immigration and naturalization matters; immigration enforcement procedures; refugees and asylum policies; travel and residence documentation; foreign labor; benefits for immigrants. 
"International Affairs": matters affecting foreign aid, human rights, international law and organizations; national governance; arms control; diplomacy and foreign officials; alliances and collective security. 
"Labor and Employment": matters affecting hiring and composition of the workforce, wages and benefits, labor-management relations; occupational safety, personnel management, unemployment compensation. 
"Law": matters affecting civil actions and administrative remedies, courts and judicial administration, general constitutional issues, dispute resolution, including mediation and arbitration. 
"Public Lands and Natural Resources": natural areas (including wilderness); lands under government jurisdiction; land use practices and policies; parks, monuments, and historic sites; fisheries and marine resources; mining and minerals. 
"Science, Technology, Communications": natural sciences, space exploration, research policy and funding, research and development, STEM education, scientific cooperation and communication; technology policies, telecommunication, information technology; digital media, journalism. 
"Taxation": all aspects of income, excise, property, inheritance, and employment taxes; tax administration and collection. 
"Transportation and Public Works": all aspects of transportation modes and conveyances, including funding and safety matters; Coast Guard; infrastructure development; travel and tourism. 
"Water Resources Development": the supply and use of water and control of water flows; watersheds; floods and storm protection; wetlands. 
Variable: policy_area
which of the policy labels from the list above apply to the text.
Variable: policy_reasoning
A 20-ish word explanation for why the text fits in the selected policy area or areas
Format response as (and remember to wrap response in quotes): 
{{
"personal_attack": yes/no,
"personal_attack_target": identity of target,
"policy_attack": yes/no,
"attack_reasoning": explanation,
"is_bipartisanship": yes/no,
"bipartisanship_reasoning": explanation,
"is_creditclaiming": yes/no,
"creditclaiming_reasoning": explanation,
"policy": yes/no,
"policy_area": [],
"policy_reasoning": explanation,
}}
Text: "{target}"
"""

def yesno(x):
    x = x.lower()
    if x == 'yes':
        return 1
    elif x == 'no':
        return 0
    else:
        return None

# def is_policy_pull(policy_area):
#     if policy_area in ['[]','','None','none',' ']:
#         return 0
#     elif policy_area == []:
#         return 0
#     else:
#         if is_valid_python_list(policy_area):
#             return 1
#         else:
#             # print('INVALID FORMAT:', policy_area)
#             return 0
#     return None

# def is_valid_python_list(string):
#     # Regex pattern to match a simple Python list
#     list_pattern = re.compile(r'^\s*\[.*\]\s*$')
#     # Check if the string matches the pattern
#     if list_pattern.match(string):
#         try:
#             parsed_value = ast.literal_eval(string)
#             return isinstance(parsed_value, list)
#         except (ValueError, SyntaxError):
#             return False
#     return False

column_map = {
    'personal_attack': {
        'name': 'attack_personal',
        'filter': lambda x: yesno(x),
    },
    'personal_attack_target': {
        'name': 'attack_target',
        'filter': lambda x: str(x),
    }, 
    'policy_attack': {
        'name': 'attack_policy',
        'filter': lambda x: yesno(x),
    },
    'attack_reasoning': {
        'name': 'attack_explanation',
        'filter': lambda x: str(x),
    },
    "is_bipartisanship": {
        "name": "outcome_bipartisanship",
        "filter": lambda x: yesno(x),
    },
    'bipartisanship_reasoning': {
        'name': 'bipartisanship_explanation',
        'filter': lambda x: str(x),
    },
    "is_creditclaiming": {
        "name": "outcome_creditclaiming",
        "filter": lambda x: yesno(x),
    },
    'creditclaiming_reasoning': {
        'name': 'creditclaiming_explanation',
        'filter': lambda x: str(x),
    },
    'policy': {
        'name': 'policy',
        'filter': lambda x: yesno(x),
    },
    'policy_reasoning': {
        'name': 'policy_explanation',
        'filter': lambda x: str(x),
    },
    'policy_area': {
        'name': 'policy_area',
        'filter': lambda x: str(x),
    },
}