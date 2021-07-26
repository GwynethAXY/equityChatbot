import json
import random
import decimal
import boto3
from decimal import Decimal

# Get the service resource.
dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

# Get the DynamoDB table.
spectrum = dynamodb.Table('SpectrumData')
companies = dynamodb.Table('companies')
keywords = dynamodb.Table('keywords')
cache = dynamodb.Table('data-cache')
key = ''
entityId = ''

###########
# Getters #
###########
def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']


def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']
    else:
        return None


def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']

    return {}

##########################
# Response Message Types #
##########################
def elicit_intent(intent_request, session_attributes, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'sessionAttributes': session_attributes
        },
        'messages': [message] if message != None else None,
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': message if type(message) is list else [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }
    return response


def successful_close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    question = 'Is there anything else that I can help you with today?'
    message = setPrefixLine(question, message)
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'

            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': message if type(message) is list else [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

#########################
# Query Database Tables #
#########################
def QuerySpectrum(key, entityId):
    return spectrum.get_item(
        Key={
            'entityId': entityId,
            'keyword': key
        }
    )


def GetKey(keyword):
    response = keywords.get_item(
        Key={
            'keyword': keyword
        }
    )
    return response['Item']['key']


def GetEntityId(company):
    response = companies.get_item(
        Key={
            'name': company
        }
    )
    return response['Item']['id']


def GetCompanyName(entityId):
    data = client.scan(
        TableName='companies',
        FilterExpression='#c = :comp',
        ExpressionAttributeValues={
            ':comp': {
                'S': entityId
            }
        },
        ExpressionAttributeNames={
            '#c': 'id'
        }
    )
    if (len(data['Items']) == 1):
        return data['Items'][0]['name']['S']

####################
# Intent Functions #
####################
def ExecuteQuery(intent_request):
    reply = ''
    sessionId = intent_request['sessionId']
    session_attributes = get_session_attributes(intent_request)
    slots = intent_request['sessionState']['intent']['slots']
    resolvedKeywords = slots['KeyWords']['value']['resolvedValues']
    resolvedCompanies = slots['CompanyName']['value']['resolvedValues']

    # CASE 1: multiple keywords, one company
    if (len(resolvedKeywords) > 1 and len(resolvedCompanies) == 1):
        cache_state(sessionId, slots)
        # output list of resolvedValues
        originalRequest = slots['KeyWords']['value']['originalValue']
        question = 'Which keyword of ' + resolvedCompanies[0] + ' are you referring to?'
        message = setPrefixLine(question, [])
        message = ChooseOneOfMany(resolvedKeywords, message)
        fulfillment_state = "Failed"
        return close(intent_request, session_attributes, fulfillment_state, message)

    # CASE 2: one keyword, multiple companies
    elif (len(resolvedKeywords) == 1 and len(resolvedCompanies) > 1):
        cache_state(sessionId, slots)
        question = 'Which company\'s ' + resolvedKeywords[0] + ' are you referring to?'
        message = setPrefixLine(question, [])
        message = ChooseOneOfMany(resolvedCompanies, message)
        fulfillment_state = "Failed"
        return close(intent_request, session_attributes, fulfillment_state, message)

    # CASE 3: multiple keywords, multiple companies
    elif (len(resolvedKeywords) > 1 and len(resolvedCompanies) > 1):
        cache_state(sessionId, slots)
        question = 'Which keyword are you referring to?'
        message = setPrefixLine(question, [])
        message = ChooseOneOfMany(resolvedKeywords, message)
        fulfillment_state = "Failed"
        return close(intent_request, session_attributes, fulfillment_state, message)

    # CASE 4: one keyword, one company
    elif (len(resolvedKeywords) == 1 and len(resolvedCompanies) == 1):
        return makeQueryToSpectrum(intent_request, resolvedKeywords[0], resolvedCompanies[0])
    else:
        raise ("need keyword and company")

def makeQueryToSpectrum(intent_request, resolvedKeyword, resolvedCompany):
    reply = ''
    session_attributes = get_session_attributes(intent_request)
    key = GetKey(resolvedKeyword)
    entityId = GetEntityId(resolvedCompany)
    response = QuerySpectrum(key, entityId)
    if 'Item' not in response:
        reply = "No Data"
    else:
        item = response['Item']
        reply = resolvedKeyword + ' of ' + resolvedCompany + ' is ' + item['value'] + ' ' + item['currencyIso']
    message = [{
        'contentType': 'PlainText',
        'content': reply
    }]
    fulfillment_state = "Fulfilled"
    return successful_close(intent_request, session_attributes, fulfillment_state, message)

def SelectOption(intent_request):
    session_attributes = get_session_attributes(intent_request)
    sessionId = intent_request['sessionId']
    selected = int(
        intent_request['sessionState']['intent']['slots']['SelectedOption']['value']['resolvedValues'][0]) - 1
    keywords = 'KeyWords'
    companies = 'CompanyName'
    optionForKeyword = False
    reply = ''

    ##check if selection is for keyword or company
    if (is_cache_resolved_values_too_big(sessionId, keywords)):
        # choice is for keyword
        optionForKeyword = True
        if (IsOutOfBounds(sessionId, keywords, selected)):
            message = setPrefixLine('Please make a selection within bound.', [])
            fulfillment_state = "Failed"
            return close(intent_request, session_attributes, fulfillment_state, message)
        else:
            selectedKeyword = get_option_from_cache(sessionId, keywords, selected)
    else:
        selectedKeyword = get_option_from_cache(sessionId, keywords, 0)

    if (is_cache_resolved_values_too_big(sessionId, companies) and not optionForKeyword):
        selectedKeyword = get_option_from_cache(sessionId, keywords, 0)

        if (IsOutOfBounds(sessionId, companies, selected)):
            message = setPrefixLine('Please make a selection within bound.', [])
            fulfillment_state = "Failed"
            return close(intent_request, session_attributes, fulfillment_state, message)
        else:
            selectedCompany = get_option_from_cache(sessionId, companies, selected)
    elif (is_cache_resolved_values_too_big(sessionId, companies) and optionForKeyword):
        update_value_in_cache(sessionId, keywords, [selectedKeyword])
        # output list of companies
        companiesList = get_from_cache(sessionId, companies)
        question = 'Which company are you referring to?'
        message = setPrefixLine(question, [])
        message = ChooseOneOfMany(companiesList, message)
        fulfillment_state = "Failed"
        return close(intent_request, session_attributes, fulfillment_state, message)
    else:
        selectedCompany = get_option_from_cache(sessionId, companies, 0)
    delete_from_cache(sessionId)
    return makeQueryToSpectrum(intent_request, selectedKeyword, selectedCompany)


def ContinueQuery(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = intent_request['sessionState']['intent']['slots']
    resolvedKeywords = slots['Continue']['value']['resolvedValues']
    if resolvedKeywords[0] == "yes":

        message = {
            'contentType': 'PlainText',
            'content': "Yup sure, input your query below"
        }

    elif resolvedKeywords[0] == "no":
        message = {
            'contentType': 'PlainText',
            'content': "Yup sure, enjoy your day. Always happy to help:)"
        }

    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }


def GetRanking(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = intent_request['sessionState']['intent']['slots']
    number = slots['Number']['value']['resolvedValues'][0]
    keywords = slots['KeyWords']['value']['resolvedValues']

    # 1 keyword detected
    if (len(keywords) == 1):
        keyword = keywords[0]
        key = GetKey(keyword)
        data = client.scan(
            TableName='SpectrumData',
            FilterExpression='#k = :val',
            ExpressionAttributeValues={
                ':val': {
                    'S': key
                }
            },
            ExpressionAttributeNames={
                '#k': 'keyword'
            }
        )
        items = data['Items']

        items.sort(key=lambda x: x['rawValue']['S'], reverse=True)
        items = items[:int(number)]
        result = []
        for i in range(len(items)):
            item = items[i]
            result.append(GetCompanyName(item['entityId']['S']) + ': ' + item['rawValue']['S'])
        answer = 'Top ' + number + ' ' + keyword + ' are:'
        message = setPrefixLine(answer, [])
        message = ChooseOneOfMany(result, message)
        fulfillment_state = "Fulfilled"
        return successful_close(intent_request, session_attributes, fulfillment_state, message)


def GetComparison(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = intent_request['sessionState']['intent']['slots']
    amount = slots['Amount']['value']['resolvedValues'][0]
    keyword = slots['KeyWords']['value']['resolvedValues'][0]
    isGreaterThan = slots['Comparator']['value']['resolvedValues'][0] == 'greater than'
    filterExpr = ''
    if (isGreaterThan):
        filterExpr = "#a >= :amt AND #k = :val"
    else:
        filterExpr = "#a <= :amt AND #k = :val"
    key = GetKey(keyword)
    data = client.scan(
        TableName='SpectrumData',
        FilterExpression=filterExpr,
        ExpressionAttributeValues={
            ':val': {
                'S': key
            },
            ':amt': {
                'S': amount
            }
        },
        ExpressionAttributeNames={
            '#k': 'keyword',
            '#a': 'rawValue'
        }
    )
    items = data['Items']
    result = []
    # for item in items:
    for i in range(len(items)):
        item = items[i]
        companyName = GetCompanyName(item['entityId']['S'])
        if companyName != None:
            result.append(companyName + ': ' + item['rawValue']['S'])
    compare = 'more than' if isGreaterThan else 'less than'
    answer = 'Companies with ' + keyword + ' ' + compare + ' ' + amount + ' are: '
    message = setPrefixLine(answer, [])
    message = ChooseOneOfMany(result, message)
    fulfillment_state = "Fulfilled"
    return successful_close(intent_request, session_attributes, fulfillment_state, message)

##################
# Intent Helpers #
##################
def setPrefixLine(line, reply):
    msg = {
        'contentType': 'PlainText',
        'content': line
    }
    reply.append(msg)
    return reply


def ChooseOneOfMany(resolvedWords, message):
    for i in range(len(resolvedWords)):
        line = str(i + 1) + ': ' + resolvedWords[i]
        msg = {
            'contentType': 'PlainText',
            'content': line
        }
        message.append(msg)
    return message


def IsOutOfBounds(sessionId, keyword, selected):
    maxLen = len(get_from_cache(sessionId, keyword))
    return (selected >= maxLen) or (selected < 0)

###################################
# Data Caching for Unique Session #
###################################
def cache_state(sessionId, state):
    item = {}
    item['sessionId'] = sessionId
    item['KeyWords'] = state['KeyWords']['value']['resolvedValues']
    item['CompanyName'] = state['CompanyName']['value']['resolvedValues']
    reparsed_state = json.loads(json.dumps(item), parse_float=Decimal)
    response = cache.put_item(
        Item=reparsed_state
    )
    return response


def get_from_cache(sessionId, keyword):
    response = cache.get_item(Key={'sessionId': sessionId})
    data = response['Item']
    return data[keyword]


def is_cache_resolved_values_too_big(sessionId, keyword):
    return len(get_from_cache(sessionId, keyword)) > 1


def get_option_from_cache(sessionId, keyword, option):  # option is an integer
    response = cache.get_item(Key={'sessionId': sessionId})
    data = response['Item']
    return data[keyword][option]


def delete_from_cache(sessionId):
    cache.delete_item(Key={'sessionId': sessionId})


def update_value_in_cache(sessionId, keyword, new_value):
    response = cache.update_item(
        Key={
            'sessionId': sessionId
        },
        UpdateExpression="set {}=:r".format(keyword),
        ExpressionAttributeValues={
            ':r': new_value
        },
        ReturnValues="UPDATED_NEW"
    )

######################
# Lambda Entry Point #
######################
def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    response = None
    # Dispatch to your bot's intent handlers
    if intent_name == 'QueryIntent':
        return ExecuteQuery(intent_request)
    elif intent_name == 'SelectOptionIntent':
        return SelectOption(intent_request)
    elif intent_name == 'ContinueIntent':
        return ContinueQuery(intent_request)
    elif intent_name == 'RankingIntent':
        return GetRanking(intent_request)
    elif intent_name == 'ComparisonIntent':
        return GetComparison(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')

def lambda_handler(event, context):
    response = dispatch(event)
    return response