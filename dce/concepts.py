#!/usr/bin/env python

# Copyright 2011 The Regents of the University of California 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "Aaron Steele (eightysteele@gmail.com)"
__copyright__ = "Copyright 2011 The Regents of the University of California"
__contributors__ = ["John Wieczorek (gtuco.btuco@gmail.com)"]

FULL_NAMES = {
    'acceptednameusage': 'anu',
    'acceptednameusageid': 'anuid',
    'accessrights': 'ar',
    'associatedmedia': 'am',
    'associatedoccurrences': 'ao',
    'associatedreferences': 'ar',
    'associatedsequences': 'as',
    'associatedtaxa': 'at',
    'basisofrecord': 'br',
    'bed': 'bd',
    'behavior': 'b',
    'bibliographiccitation': 'bc',
    'catalognumber': 'cat',
    'class': 'cl',
    'collectioncode': 'cc',
    'collectionid': 'cid',
    'continent': 'ct',
    'coordinateprecision': 'cp',
    'coordinateuncertaintyinmeters': 'cu',
    'country': 'cn',
    'countrycode': 'cnc',
    'county': 'co',
    'datageneralizations': 'dg',
    'datasetid': 'did',
    'datasetname': 'dn',
    'dateidentified': 'di',
    'day': 'd',
    'decimallatitude': 'dlat',
    'decimallongitude': 'dlng',
    'disposition': 'dsp',
    'dynamicproperties': 'dp',
    'earliestageorloweststage': 'eage',
    'earliesteonorlowesteonothem': 'eeon',
    'earliestepochorlowestseries': 'eep',
    'earliesteraorlowesterathem': 'eera',
    'earliestperiodorlowestsystem': 'ep',
    'enddayofyear': 'edy',
    'establishmentmeans': 'em',
    'eventdate': 'ed',
    'eventid': 'eid',
    'eventremarks': 'er',
    'eventtime': 'et',
    'family': 'fm',
    'fieldnotes': 'fnt',
    'fieldnumber': 'fnm',
    'footprintspatialfit': 'fpsf',
    'footprintsrs': 'fps',
    'footprintwkt': 'fp',
    'formation': 'frm',
    'genus': 'g',
    'geodeticdatum': 'gd',
    'geologicalcontextid': 'gid',
    'georeferencedby': 'gb',
    'georeferenceprotocol': 'gp',
    'georeferenceremarks': 'gr',
    'georeferencesources': 'gs',
    'georeferenceverificationstatus': 'gvs',
    'group': 'grp',
    'habitat': 'h',
    'higherclassification': 'hc',
    'highergeography': 'hg',
    'highergeographyid': 'hgid',
    'highestbiostratigraphiczone': 'hbz',
    'identificationid': 'idid',
    'identificationqualifier': 'iq',
    'identificationreferences': 'irf',
    'identificationremarks': 'irm',
    'identifiedby': 'ib',
    'individualcount': 'ic',
    'individualid': 'indid',
    'informationwithheld': 'iw',
    'infraspecificepithet': 'ise',
    'institutioncode': 'ic',
    'institutionid': 'iid',
    'island': 'i',
    'islandgroup': 'ig',
    'kingdom': 'k',
    'language': 'ln',
    'latestageorhigheststage': 'lage',
    'latesteonorhighesteonothem': 'leon',
    'latestepochorhighestseries': 'lep',
    'latesteraorhighesterathem': 'lera',
    'latestperiodorhighestsystem': 'lp',
    'lifestage': 'ls',
    'lithostratigraphicterms': 'lt',
    'locality': 'l',
    'locationaccordingto': 'lcat',
    'locationid': 'lid',
    'locationremarks': 'lr',
    'lowestbiostratigraphiczone': 'lbz',
    'maximumdepthinmeters': 'mxd',
    'maximumdistanceabovesurfaceinmeters': 'mxa',
    'maximumelevationinmeters': 'mxe',
    'member': 'mem',
    'minimumdepthinmeters': 'mnd',
    'minimumdistanceabovesurfaceinmeters': 'mna',
    'minimumelevationinmeters': 'mne',
    'modified': 'md',
    'month': 'm',
    'municipality': 'mn',
    'nameaccordingto': 'nat',
    'nameaccordingtoid': 'natid',
    'namepublishedin': 'np',
    'namepublishedinid': 'npid',
    'nomenclaturalcode': 'nc',
    'nomenclaturalstatus': 'ns',
    'occurrencedetails': 'od',
    'occurrenceid': 'oid',
    'occurrenceremarks': 'or',
    'occurrencestatus': 'os',
    'order': 'ord',
    'originalnameusage': 'onu',
    'originalnameusageid': 'onuid',
    'othercatalognumbers': 'ocn',
    'ownerinstitutioncode': 'oic',
    'parentnameusage': 'pnu',
    'parentnameusageid': 'pnuid',
    'phylum': 'ph',
    'pointradiusspatialfit': 'prsf',
    'preparations': 'p',
    'previousidentifications': 'pi',
    'recordedby': 'rb',
    'recordnumber': 'rn',
    'reproductivecondition': 'rc',
    'rights': 'r',
    'rightsholder': 'rh',
    'samplingeffort': 'sme',
    'samplingprotocol': 'smp',
    'scientificname': 'sn',
    'scientificnameauthorship': 'sna',
    'scientificnameid': 'sid',
    'sex': 'sx',
    'specificepithet': 'se',
    'startdayofyear': 'sdy',
    'stateprovince': 'sp',
    'subgenus': 'sg',
    'taxonconceptid': 'tcid',
    'taxonid': 'tid',
    'taxonomicstatus': 'ts',
    'taxonrank': 'tr',
    'taxonremarks': 'trm',
    'type': 't',
    'typestatus': 'ts',
    'verbatimcoordinates': 'vc',
    'verbatimcoordinatesystem': 'vcs',
    'verbatimdepth': 'vd',
    'verbatimelevation': 've',
    'verbatimeventdate': 'ved',
    'verbatimlatitude': 'vlat',
    'verbatimlocality': 'vl',
    'verbatimlongitude': 'vlng',
    'verbatimsrs': 'vs',
    'verbatimtaxonrank': 'vtr',
    'vernacularname': 'vn',
    'waterbody': 'w',
    'year': 'y'
}

SHORT_NAMES = {
    'am': 'associatedmedia',
    'anu': 'acceptednameusage',
    'anuid': 'acceptednameusageid',
    'ao': 'associatedoccurrences',
    'ar': 'accessrights',
    'as': 'associatedsequences',
    'at': 'associatedtaxa',
    'b': 'behavior',
    'bc': 'bibliographiccitation',
    'bd': 'bed',
    'br': 'basisofrecord',
    'cat': 'catalognumber',
    'cc': 'collectioncode',
    'cid': 'collectionid',
    'cl': 'class',
    'cn': 'country',
    'cnc': 'countrycode',
    'co': 'county',
    'cp': 'coordinateprecision',
    'ct': 'continent',
    'cu': 'coordinateuncertaintyinmeters',
    'd': 'day',
    'dg': 'datageneralizations',
    'di': 'dateidentified',
    'did': 'datasetid',
    'dlat': 'decimallatitude',
    'dlng': 'decimallongitude',
    'dn': 'datasetname',
    'dp': 'dynamicproperties',
    'dsp': 'disposition',
    'eage': 'earliestageorloweststage',
    'ed': 'eventdate',
    'edy': 'enddayofyear',
    'eeon': 'earliesteonorlowesteonothem',
    'eep': 'earliestepochorlowestseries',
    'eera': 'earliesteraorlowesterathem',
    'eid': 'eventid',
    'em': 'establishmentmeans',
    'ep': 'earliestperiodorlowestsystem',
    'er': 'eventremarks',
    'et': 'eventtime',
    'fm': 'family',
    'fnm': 'fieldnumber',
    'fnt': 'fieldnotes',
    'fp': 'footprintwkt',
    'fps': 'footprintsrs',
    'fpsf': 'footprintspatialfit',
    'frm': 'formation',
    'g': 'genus',
    'gb': 'georeferencedby',
    'gd': 'geodeticdatum',
    'gid': 'geologicalcontextid',
    'gp': 'georeferenceprotocol',
    'gr': 'georeferenceremarks',
    'grp': 'group',
    'gs': 'georeferencesources',
    'gvs': 'georeferenceverificationstatus',
    'h': 'habitat',
    'hbz': 'highestbiostratigraphiczone',
    'hc': 'higherclassification',
    'hg': 'highergeography',
    'hgid': 'highergeographyid',
    'i': 'island',
    'ib': 'identifiedby',
    'ic': 'individualcount',
    'idid': 'identificationid',
    'ig': 'islandgroup',
    'iid': 'institutionid',
    'indid': 'individualid',
    'iq': 'identificationqualifier',
    'irf': 'identificationreferences',
    'irm': 'identificationremarks',
    'ise': 'infraspecificepithet',
    'iw': 'informationwithheld',
    'k': 'kingdom',
    'l': 'locality',
    'lage': 'latestageorhigheststage',
    'lbz': 'lowestbiostratigraphiczone',
    'lcat': 'locationaccordingto',
    'leon': 'latesteonorhighesteonothem',
    'lep': 'latestepochorhighestseries',
    'lera': 'latesteraorhighesterathem',
    'lid': 'locationid',
    'ln': 'language',
    'lp': 'latestperiodorhighestsystem',
    'lr': 'locationremarks',
    'ls': 'lifestage',
    'lt': 'lithostratigraphicterms',
    'm': 'month',
    'md': 'modified',
    'mem': 'member',
    'mn': 'municipality',
    'mna': 'minimumdistanceabovesurfaceinmeters',
    'mnd': 'minimumdepthinmeters',
    'mne': 'minimumelevationinmeters',
    'mxa': 'maximumdistanceabovesurfaceinmeters',
    'mxd': 'maximumdepthinmeters',
    'mxe': 'maximumelevationinmeters',
    'nat': 'nameaccordingto',
    'natid': 'nameaccordingtoid',
    'nc': 'nomenclaturalcode',
    'np': 'namepublishedin',
    'npid': 'namepublishedinid',
    'ns': 'nomenclaturalstatus',
    'ocn': 'othercatalognumbers',
    'od': 'occurrencedetails',
    'oic': 'ownerinstitutioncode',
    'oid': 'occurrenceid',
    'onu': 'originalnameusage',
    'onuid': 'originalnameusageid',
    'or': 'occurrenceremarks',
    'ord': 'order',
    'os': 'occurrencestatus',
    'p': 'preparations',
    'ph': 'phylum',
    'pi': 'previousidentifications',
    'pnu': 'parentnameusage',
    'pnuid': 'parentnameusageid',
    'prsf': 'pointradiusspatialfit',
    'r': 'rights',
    'rb': 'recordedby',
    'rc': 'reproductivecondition',
    'rh': 'rightsholder',
    'rn': 'recordnumber',
    'sdy': 'startdayofyear',
    'se': 'specificepithet',
    'sg': 'subgenus',
    'sid': 'scientificnameid',
    'sme': 'samplingeffort',
    'smp': 'samplingprotocol',
    'sn': 'scientificname',
    'sna': 'scientificnameauthorship',
    'sp': 'stateprovince',
    'sx': 'sex',
    't': 'type',
    'tcid': 'taxonconceptid',
    'tid': 'taxonid',
    'tr': 'taxonrank',
    'trm': 'taxonremarks',
    'ts': 'taxonomicstatus',
    'vc': 'verbatimcoordinates',
    'vcs': 'verbatimcoordinatesystem',
    'vd': 'verbatimdepth',
    've': 'verbatimelevation',
    'ved': 'verbatimeventdate',
    'vl': 'verbatimlocality',
    'vlat': 'verbatimlatitude',
    'vlng': 'verbatimlongitude',
    'vn': 'vernacularname',
    'vs': 'verbatimsrs',
    'vtr': 'verbatimtaxonrank',
    'w': 'waterbody',
    'y': 'year'
}

def names():
    return FULL_NAMES.keys()

def short_names():
    return SHORT_NAMES.keys()

def get_short_name(name):
    return FULL_NAMES.get(name, None)

def get_name(short_name):
    return SHORT_NAMES.get(short_name, None)

def is_name(name):
    return FULL_NAMES.has_key(name)

def is_short_name(short_name):
    return SHORT_NAMES.has_key(short_name)
