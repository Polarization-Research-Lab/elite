import re, ast
import llms
import hjson

# System prompt - contains all the classification instructions
system_prompt = """
You are a political text classifier. Evaluate the provided text for the following tasks and return results in JSON format.

Evaluate each of the following tasks sequentially:

Attacks: 

Task: Assess the text for a personal_attack (output either "yes" or "no"). If "yes", specify both the attack_type and personal_attack_target as described below. If "no", set "attack_type" and "personal_attack_target" as null.
Definition of Personal Attack (attack_type): A personal attack explicitly questions an individual's character, integrity, intelligence, morality, or patriotism. Opposition to an individual or their policy choices does not count. Personal attacks must be directly and explicitly critical of traits like integrity or loyalty in a way that cannot be interpreted as policy disagreement. Statements implying a lack of interest or attention to a policy issue are not considered personal attacks. Opposition to an individual's professional decisions, official actions, or leadership in their official role does not count. 
character: Explicitly saying someone as lazy, unreliable, stupid, etc.
integrity: Explicit accusations that someone is dishonest, lies, embellishes the truth, or lacks integrity.
intelligence: Claims that someone is mentally unfit, unintelligent, incompetent, or otherwise mentally incapable.
morality: Accusations of corruption, unethical behavior, criminality, or fraud.
patriotism: Accusations that someone is un-American, anti-democratic, pro-terrorist, or not supportive of U.S. interests, democracy, or national security.
Target Identification (personal_attack_target): 
Record the name of the person attacked, excluding any titles (e.g., "Joe Biden" instead of "President Joe Biden"). Use no more than two words for the name.
Exclusion criteria. Do not consider statements as personal attacks if they:
Critique job performance, policies, legislation, or appointments without explicitly questioning personal character traits such as loyalty or integrity or name calling
Require assumptions or interpretations 
Question leadership decisions
Question someone's support for US policy, foreign policy or an ally
Are not explicit and overt in questioning an individual's personal traits.
Critique someone's official role in government 
Merely respond to or mention an attack.
Refer to an attack in a general sense, such as on "attackers," rather than individuals.
Are quoted attacks rather than direct statements by the speaker.
Refer to attacks on foreign lobbyists, agents, or external groups (e.g., companies, organizations, foreign leaders like Vladimir Putin, or terrorist groups).
Critique job performance, policies, or legislation without explicitly questioning personal character traits.
Conflate personal and policy failings
Blame someone for a policy failure without making a personal attack
Only include statements as attacks if they meet all conditions outlined above

Extremism:
Task: Assess the text for extreme_label (output either "yes" or "no"). If "yes", ensure the text meets the criteria below. If "no", output "extreme_label":"no".
Definition: A text contains an extreme label if it calls an American politician, the Democratic or Republican party, or a policy: far left, far right, MAGA, extreme, radical, woke, fascist, or communist.
extreme_target: If extreme_label is "yes", identify the person(s) or group(s) described as extreme. Use no more than two words per entry, and remove any titles.

Policy criticism:

Task: Assess the text for a policy_attack (output either "yes" or "no"). If "yes", ensure the text meets the criteria below. If "no", output "policy_attack": "no".
Definition of Policy Attack: A policy_attack is a constructive critique of policy, legislation, or court decisions. To qualify as a policy attack, the text must:
Object to or raise concerns about a specific policy, law, or court ruling.
Use fact-based arguments, even if critical or negative.
Avoid emotional appeals, inflammatory language, claims of extremism, or personal attacks on individuals involved with the policy including accusing them of lying of withholding information.Avoid emotional appeals, inflammatory language, or personal attacks on individuals involved with the policy.
Exclude statements where you identified personal_attack was "yes"

Bipartisanship:

Task: Assess the text for is_bipartisanship (output either "yes" or "no").
Definition of Bipartisanship: Bipartisanship, "working across the aisle," or collaboration between Democrats and Republicans. Emphasis on cooperation, compromise, or finding common ground across party lines.

Credit claiming:

Task: Assess the text for is_creditclaiming (output either "yes" or "no").
Definition of Credit-Claiming:
    •   Creating or passing legislation.
    •   Securing government spending, grants, or funding.
    •   Emphasizing personal or party accomplishments in office.

Policy

Task: Asses if policy is discussed and determine the policy_area from the list below.
Definition: Evaluate whether the text discusses public policy by identifying any references to specific policies, laws, legislation, or general policy areas (e.g., healthcare, education, national security). Exclude procedural or operational statements that do not clearly address a particular policy area.
Here are a list of policy areas:

"Agriculture and Food": agricultural practices; agricultural prices and marketing; agricultural education; food assistance or nutrition programs; food industry, supply, and safety; aquaculture; horticulture and plants. 
"Armed Forces and National Security": military operations and spending, facilities, procurement and weapons, personnel, intelligence; strategic materials; war and emergency powers; veterans' issues. 
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

Return your response in this exact JSON format:

{
    "attacks": {
        "personal_attack": ,
        "attack_type": [],
        "personal_attack_target": ,
    },
    "extremism": {
        "extreme_label":,
        "extreme_target":,
    },
    "policy_criticism": {
        "policy_attack": ,
    },
    "bipartisanship": {
        "is_bipartisanship": ,
    },
    "credit_claiming": {
        "is_creditclaiming": ,
    },
    "policy": {
        "policy_area": [],
    }
}
"""

# User prompt template - just the text to analyze
def get_user_prompt(text):
    """Generate user prompt with the text to analyze"""
    # Defensive check for empty/null text
    if not text or str(text).strip() == '' or str(text).strip().lower() == 'nan':
        raise ValueError(f"Empty or invalid text provided: {repr(text)}")
    return f"Analyze this text: {text}"

def pipeline_optimized(row):
    """Optimized pipeline using system prompt"""
    new_row = row.copy()
    new_row['classified'] = 0
    
    try:
        user_message = get_user_prompt(new_row['text'])
        response = llms.chatgpt_with_system(user_message, system_prompt)
        
        if response.lstrip().startswith("```json"): 
            response = response.lstrip()[7:]
        if response.rstrip().endswith("```"): 
            response = response.rstrip()[:-3]

        try:
            response = hjson.loads(response)
        except:
            print('bad json detected')
            return new_row

        new_row['attack_personal'] = yesno(response['attacks']['personal_attack'])
        new_row['attack_type'] = str(response['attacks']['attack_type'])
        new_row['attack_target'] = str(response['attacks']['personal_attack_target'])
        new_row['attack_policy'] = yesno(response['policy_criticism']['policy_attack'])
        new_row['outcome_bipartisanship'] = yesno(response['bipartisanship']['is_bipartisanship'])
        new_row['outcome_creditclaiming'] = yesno(response['credit_claiming']['is_creditclaiming'])
        new_row['policy_area'] = str(response['policy']['policy_area'])
        new_row['extreme_label'] = str(response['extremism']['extreme_label'])
        new_row['extreme_target'] = str(response['extremism']['extreme_target'])

        if len(hjson.loads(new_row['policy_area'])) > 0:
            new_row['policy'] = 1
        else:
            new_row['policy'] = 0

        new_row['classified'] = 1

    except Exception as e:
        print(f'error with {new_row["id"]}, {new_row["text"]}: {e}')

    return new_row

def yesno(x):
    if x:
        x = x.lower()
        if x == 'yes':
            return 1
        elif x == 'no':
            return 0
        else:
            return None

# Main pipeline function
pipeline = pipeline_optimized 