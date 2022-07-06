"""Data model"""

COLNAMES = {
    'publication': {
        'consortium': 'Publication Consortium Name',
        'theme': 'Publication Theme Name',
        'doi': 'Publication Doi',
        'journal': 'Publication Journal',
        'pubMedId': 'Pubmed Id',
        'pubMedUrl': 'Pubmed Url',
        'publicationTitle': 'Publication Title',
        'publicationYear': 'Publication Year',
        'keywords': 'Publication Keywords',
        'authors': 'Publication Authors',
        'assay': 'Publication Assay',
        'tumorType': 'Publication Tumor Type',
        'tissue': 'Publication Tissue',
        'dataset': 'Publication Dataset Alias'
    },
    'dataset': {
        'pubMedId': 'Dataset Pubmed Id',
        'consortium': 'Dataset Consortium Name',
        'theme': 'Dataset Theme Name',
        'datasetName': 'Dataset Name',
        'datasetAlias': 'Dataset Alias',
        'description': 'Dataset Description',
        'overallDesign': 'Dataset Design',
        'assay': 'Dataset Assay',
        'species': 'Dataset Species',
        'tumorType': 'Dataset Tumor Type',
        'externalLink': 'Dataset External Link'
    },
    'tool': {
        'pubMedId': 'Tool Pubmed Id',
        'consortium': 'Tool Consortium Name',
        'toolName': 'Tool Name',
        'description': 'Tool Description',
        'homepage': 'Tool Homepage',
        'version': 'Tool Version',
        'operation': 'Tool Operation',
        'inputData': 'Tool Input Data',
        'outputData': 'Tool Output Data',
        'inputFormat': 'Tool Input Format',
        'outputFormat': 'Tool Output Format',
        'functionNote': 'Tool Function Note',
        'cmd': 'Tool Cmd',
        'toolType': 'Tool Type',
        'topic': 'Tool Topic',
        'operatingSystem': 'Tool Operating System',
        'language': 'Tool Language',
        'license': 'Tool License',
        'cost': 'Tool Cost',
        'accessibility': 'Tool Accessibility',
        'downloadUrl': 'Tool Download Url',
        'downloadType': 'Tool Download Type',
        'downloadNote': 'Tool Download Note',
        'downloadVersion': 'Tool Download Version',
        'documentationUrl': 'Tool Documentation Url',
        'documentationType': 'Tool Documentation Type',
        'documentationNote': 'Tool Documentation Note',
        'linkUrl': 'Tool Link Url',
        'linkType': 'Tool Link Type',
        'linkNote': 'Tool Link Note'
    }
}
DROP = {
    'publication': [
        'publicationId', 'pubMedLink', 'themeId', 'consortiumId',
        'grantId', 'grantName', 'grantInstitution', 'datasetId'
    ],
    'dataset': [
        'datasetId', 'themeId', 'consortiumId', 'grantId', 'grantName',
        'publicationId', 'publicationTitle', 'publication'
    ],
    'tool': [
        'toolId', 'grantId', 'grantName', 'publicationTitle',
        'downloadLink', 'portalDisplay'
    ]
}
ADD = {
    'publication': [
        {'index': 15, 'colname': 'Publication Tool Name', 'value': ""},
        {'index': 0, 'colname': 'Component', 'value': "PublicationView"},
    ],
    'dataset': [
        {'index': 12, 'colname': 'Dataset Tissue', 'value': ""},
        {'index': 0, 'colname': 'Component', 'value': "DatasetView"},
    ],
    'tool': [
        {'index': 2, 'colname': 'Tool Dataset Alias', 'value': ""},
        {'index': 0, 'colname': 'Component', 'value': "ToolView"},
    ]
}
