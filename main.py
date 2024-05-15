import re
import json
import logging
import jmespath

from copy import deepcopy
from requests import Session
from json import JSONDecodeError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('houzz_crawler')

search_api_url = "https://www.houzz.com/professionals/general-contractor/california-md-us-probr0-bo~t_11786~r_4350049"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.houzz.com/professionals/general-contractor/california-md-us-probr0-bo~t_11786~r_4350049?fi=15",
    "X-Requested-With": "XMLHttpRequest",
    "x-hz-request": "true",
    "x-hz-spf-request": "true",
    "X-SPF-Referer": "https://www.houzz.com/professionals/general-contractor/california-md-us-probr0-bo~t_11786~r_4350049?fi=15",
    "X-SPF-Previous": "https://www.houzz.com/professionals/general-contractor/california-md-us-probr0-bo~t_11786~r_4350049?fi=15",
    "Connection": "keep-alive"
}

unique_ids = set()
result_data = {}
limit = 150
page_size = 15
offset_suffix = '&fi='


def main(current_url, headers, session):
    resp_json, current_offset = process_requests(current_url, headers, session)

    while len(result_data) < limit:
        logger.info(f'One more request to go! Current offset is {current_offset}')
        parse_data(resp_json)
        next_offset = current_offset+page_size
        if current_offset == 0:
            new_url = current_url+offset_suffix+str(next_offset)
        else:    
            new_url = re.sub(r'fi=(\d+)', f'fi={next_offset}', current_url)
            
        new_headers = deepcopy(headers)
        new_headers['Referer'] = current_url
        new_headers['X-SPF-Referer'] = current_url
        new_headers['X-SPF-Previous'] = current_url
        
        # Recursion until requirements won't met
        main(new_url, new_headers, session)

def process_requests(url, headers, session):
    response = session.request("GET", url, headers=headers)
    try:
        resp_json = json.loads(response.text)
        offset = re.findall(r'fi=(\d+)', url)
        if offset:
            curr_offset = int(offset[0])
        else:
            curr_offset = 0
        return resp_json, curr_offset
    except JSONDecodeError:
        return
         
def parse_data(resp_json):
    if resp_json:
        contractor_data = jmespath.search('ctx.data.stores.data.ProfessionalStore.data', resp_json)
        for contractor in contractor_data.values():
            profile_path = jmespath.search("webProTrackInfo.profileClk.path",contractor)
            profile_url = f'https://www.houzz.com{profile_path}' if profile_path else ''
            contractor_id = contractor.get('userId')
            
            profile_badges =jmespath.search('highlightBadges.profileBadges', contractor)
            normalized_p_badges = {}
            if profile_badges:
                normalized_p_badges = {badge.get('id'): normalize_badge(badge) for badge in profile_badges}
            merit_profile_badges = jmespath.search('highlightBadges.meritProfileBadges', contractor)
            normalized_m_p_badges = {}
            if merit_profile_badges:
                normalized_m_p_badges = {badge.get('id'): normalize_badge(badge) for badge in merit_profile_badges}
            
            contractor_dict = {
            'contractor_id': contractor_id,
            'professional_id':contractor.get('professionalId'),
            'address':contractor.get('address').strip(),
            'region':contractor.get('location'),
            'locality': contractor.get('city'),
            'state':contractor.get('state'),
            'postcode':contractor.get('zip'),
            'coordinates':{'latitude': contractor.get('latitude'), 'longitude': contractor.get('longitude')},
            'reviews':{'num_reviews': contractor.get('numReviews'), 'rating': contractor.get('reviewRating'), 'recent_review': contractor.get('mostRecentReview'), 'featured_review':contractor.get('featuredReview')},
            'pro_sku_id':contractor.get('proSkuId'),
            'phone':contractor.get('formattedPhone'),
            'description':contractor.get('aboutMe'),
            'display_name':contractor.get('proTypeDisplayName'),
            'badges': {
                'profile_badges': normalized_p_badges,
                'merit_profile_badges': normalized_m_p_badges},
            'budget_levels': contractor.get('budgetLevels'),
            'video_consultation_enabled': contractor.get('isVideoConsultationEnabled'),
            'verified_license': contractor.get('hasVerifiedLicense'),
            'has_kyc': contractor.get('hasVerifiedKyc'),
            'is_inactive': contractor.get('isInactivePro'),
            'profile_url': profile_url}
            
            if contractor_id not in unique_ids:
                unique_ids.add(contractor_id)
                result_data[contractor_id] = contractor_dict
    
        logger.info(f'Data chunk parsed successfully! Proceeding to the next part!')

def normalize_badge(badge):
    return {
        'title': badge.get('title'),
        'description': badge.get('description'),
        'awarded_on_date': badge.get('awardedOnDate'),
        'modified': badge.get('modified')
    }

def generate_json():
    logger.info(f'There are {len(result_data)} contractors in the result JSON!')
    json_data = json.dumps(result_data)
    with open('result.json', 'w') as output_file:
        output_file.write(json_data)

if __name__ == "__main__":
    logger.info(f'Starting scraping task! Limit of requests: {limit}')
    session = Session()
    # We need this request to get cookies so website won't reject our next requests
    make_first_request = process_requests(search_api_url, headers, session)
    # Adding mandatory param
    updated_url = search_api_url+'?spf=navigate'
    # Starting requests
    main(updated_url, headers, session)
    logger.info('Writing to the file...')
    generate_json()
    logger.info('Task finished. Result data was saved into result.json')
