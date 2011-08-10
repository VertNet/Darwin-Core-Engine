# Copyright 2011 Aaron Steele
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

__author__ = "Aaron Steele"

"""This module contains common classes and functions."""

import os

from google.appengine.ext import webapp

# Map darwin core terms to corresponding alias
DWC_TO_ALIAS = {
    'acceptednameusage': 'anu,',
    'acceptednameusageid': 'anuid,',
    'accessrights': 'ar,',
    'associatedmedia': 'am,',
    'associatedoccurrences': 'ao,',
    'associatedreferences': 'ar,',
    'associatedsequences': 'as,',
    'associatedtaxa': 'at,',
    'basisofrecord': 'br,',
    'bed': 'bd,',
    'behavior': 'b,',
    'bibliographiccitation': 'bc,',
    'catalognumber': 'cat,',
    'class': 'cl,',
    'collectioncode': 'cc,',
    'collectionid': 'cid,',
    'continent': 'ct,',
    'coordinateprecision': 'cp,',
    'coordinateuncertaintyinmeters': 'cu,',
    'country': 'cn,',
    'countrycode': 'cnc,',
    'county': 'co,',
    'datageneralizations': 'dg,',
    'datasetid': 'did,',
    'datasetname': 'dn,',
    'dateidentified': 'di,',
    'day': 'd,',
    'decimallatitude': 'dlat,',
    'decimallongitude': 'dlng,',
    'disposition': 'dsp,',
    'dynamicproperties': 'dp,',
    'earliestageorloweststage': 'eage,',
    'earliesteonorlowesteonothem': 'eeon,',
    'earliestepochorlowestseries': 'eep,',
    'earliesteraorlowesterathem': 'eera,',
    'earliestperiodorlowestsystem': 'ep,',
    'enddayofyear': 'edy,',
    'establishmentmeans': 'em,',
    'eventdate': 'ed,',
    'eventid': 'eid,',
    'eventremarks': 'er,',
    'eventtime': 'et,',
    'family': 'fm,',
    'fieldnotes': 'fnt,',
    'fieldnumber': 'fnm,',
    'footprintspatialfit': 'fpsf,',
    'footprintsrs': 'fps,',
    'footprintwkt': 'fp,',
    'formation': 'frm,',
    'genus': 'g,',
    'geodeticdatum': 'gd,',
    'geologicalcontextid': 'gid,',
    'georeferencedby': 'gb,',
    'georeferenceprotocol': 'gp,',
    'georeferenceremarks': 'gr,',
    'georeferencesources': 'gs,',
    'georeferenceverificationstatus': 'gvs,',
    'group': 'grp,',
    'habitat': 'h,',
    'higherclassification': 'hc,',
    'highergeography': 'hg,',
    'highergeographyid': 'hgid,',
    'highestbiostratigraphiczone': 'hbz,',
    'identificationid': 'idid,',
    'identificationqualifier': 'iq,',
    'identificationreferences': 'irf,',
    'identificationremarks': 'irm,',
    'identifiedby': 'ib,',
    'individualcount': 'ic,',
    'individualid': 'indid,',
    'informationwithheld': 'iw,',
    'infraspecificepithet': 'ise,',
    'institutioncode': 'ic,',
    'institutionid': 'iid,',
    'island': 'i,',
    'islandgroup': 'ig,',
    'kingdom': 'k,',
    'language': 'ln,',
    'latestageorhigheststage': 'lage,',
    'latesteonorhighesteonothem': 'leon,',
    'latestepochorhighestseries': 'lep,',
    'latesteraorhighesterathem': 'lera,',
    'latestperiodorhighestsystem': 'lp,',
    'lifestage': 'ls,',
    'lithostratigraphicterms': 'lt,',
    'locality': 'l,',
    'locationaccordingto': 'lcat,',
    'locationid': 'lid,',
    'locationremarks': 'lr,',
    'lowestbiostratigraphiczone': 'lbz,',
    'maximumdepthinmeters': 'mxd,',
    'maximumdistanceabovesurfaceinmeters': 'mxa,',
    'maximumelevationinmeters': 'mxe,',
    'member': 'mem,',
    'minimumdepthinmeters': 'mnd,',
    'minimumdistanceabovesurfaceinmeters': 'mna,',
    'minimumelevationinmeters': 'mne,',
    'modified': 'md,',
    'month': 'm,',
    'municipality': 'mn,',
    'nameaccordingto': 'nat,',
    'nameaccordingtoid': 'natid,',
    'namepublishedin': 'np,',
    'namepublishedinid': 'npid,',
    'nomenclaturalcode': 'nc,',
    'nomenclaturalstatus': 'ns,',
    'occurrencedetails': 'od,',
    'occurrenceid': 'oid,',
    'occurrenceremarks': 'or,',
    'occurrencestatus': 'os,',
    'order': 'ord,',
    'originalnameusage': 'onu,',
    'originalnameusageid': 'onuid,',
    'othercatalognumbers': 'ocn,',
    'ownerinstitutioncode': 'oic,',
    'parentnameusage': 'pnu,',
    'parentnameusageid': 'pnuid,',
    'phylum': 'ph,',
    'pointradiusspatialfit': 'prsf,',
    'preparations': 'p,',
    'previousidentifications': 'pi,',
    'recordedby': 'rb,',
    'recordnumber': 'rn,',
    'reproductivecondition': 'rc,',
    'rights': 'r,',
    'rightsholder': 'rh,',
    'samplingeffort': 'sme,',
    'samplingprotocol': 'smp,',
    'scientificname': 'sn,',
    'scientificnameauthorship': 'sna,',
    'scientificnameid': 'sid,',
    'sex': 'sx,',
    'specificepithet': 'se,',
    'startdayofyear': 'sdy,',
    'stateprovince': 'sp,',
    'subgenus': 'sg,',
    'taxonconceptid': 'tcid,',
    'taxonid': 'tid,',
    'taxonomicstatus': 'ts,',
    'taxonrank': 'tr,',
    'taxonremarks': 'trm,',
    'type': 't,',
    'typestatus': 'ts,',
    'verbatimcoordinates': 'vc,',
    'verbatimcoordinatesystem': 'vcs,',
    'verbatimdepth': 'vd,',
    'verbatimelevation': 've,',
    'verbatimeventdate': 'ved,',
    'verbatimlatitude': 'vlat,',
    'verbatimlocality': 'vl,',
    'verbatimlongitude': 'vlng,',
    'verbatimsrs': 'vs,',
    'verbatimtaxonrank': 'vtr,',
    'vernacularname': 'vn,',
    'waterbody': 'w,',
    'year': 'y'
}

# Map darwin core alias to corresponding name
ALIAS_TO_DWC = {
    'am,': 'associatedmedia',
    'anu,': 'acceptednameusage',
    'anuid,': 'acceptednameusageid',
    'ao,': 'associatedoccurrences',
    'ar,': 'accessrights',
    'as,': 'associatedsequences',
    'at,': 'associatedtaxa',
    'b,': 'behavior',
    'bc,': 'bibliographiccitation',
    'bd,': 'bed',
    'br,': 'basisofrecord',
    'cat,': 'catalognumber',
    'cc,': 'collectioncode',
    'cid,': 'collectionid',
    'cl,': 'class',
    'cn,': 'country',
    'cnc,': 'countrycode',
    'co,': 'county',
    'cp,': 'coordinateprecision',
    'ct,': 'continent',
    'cu,': 'coordinateuncertaintyinmeters',
    'd,': 'day',
    'dg,': 'datageneralizations',
    'di,': 'dateidentified',
    'did,': 'datasetid',
    'dlat,': 'decimallatitude',
    'dlng,': 'decimallongitude',
    'dn,': 'datasetname',
    'dp,': 'dynamicproperties',
    'dsp,': 'disposition',
    'eage,': 'earliestageorloweststage',
    'ed,': 'eventdate',
    'edy,': 'enddayofyear',
    'eeon,': 'earliesteonorlowesteonothem',
    'eep,': 'earliestepochorlowestseries',
    'eera,': 'earliesteraorlowesterathem',
    'eid,': 'eventid',
    'em,': 'establishmentmeans',
    'ep,': 'earliestperiodorlowestsystem',
    'er,': 'eventremarks',
    'et,': 'eventtime',
    'fm,': 'family',
    'fnm,': 'fieldnumber',
    'fnt,': 'fieldnotes',
    'fp,': 'footprintwkt',
    'fps,': 'footprintsrs',
    'fpsf,': 'footprintspatialfit',
    'frm,': 'formation',
    'g,': 'genus',
    'gb,': 'georeferencedby',
    'gd,': 'geodeticdatum',
    'gid,': 'geologicalcontextid',
    'gp,': 'georeferenceprotocol',
    'gr,': 'georeferenceremarks',
    'grp,': 'group',
    'gs,': 'georeferencesources',
    'gvs,': 'georeferenceverificationstatus',
    'h,': 'habitat',
    'hbz,': 'highestbiostratigraphiczone',
    'hc,': 'higherclassification',
    'hg,': 'highergeography',
    'hgid,': 'highergeographyid',
    'i,': 'island',
    'ib,': 'identifiedby',
    'ic,': 'individualcount',
    'idid,': 'identificationid',
    'ig,': 'islandgroup',
    'iid,': 'institutionid',
    'indid,': 'individualid',
    'iq,': 'identificationqualifier',
    'irf,': 'identificationreferences',
    'irm,': 'identificationremarks',
    'ise,': 'infraspecificepithet',
    'iw,': 'informationwithheld',
    'k,': 'kingdom',
    'l,': 'locality',
    'lage,': 'latestageorhigheststage',
    'lbz,': 'lowestbiostratigraphiczone',
    'lcat,': 'locationaccordingto',
    'leon,': 'latesteonorhighesteonothem',
    'lep,': 'latestepochorhighestseries',
    'lera,': 'latesteraorhighesterathem',
    'lid,': 'locationid',
    'ln,': 'language',
    'lp,': 'latestperiodorhighestsystem',
    'lr,': 'locationremarks',
    'ls,': 'lifestage',
    'lt,': 'lithostratigraphicterms',
    'm,': 'month',
    'md,': 'modified',
    'mem,': 'member',
    'mn,': 'municipality',
    'mna,': 'minimumdistanceabovesurfaceinmeters',
    'mnd,': 'minimumdepthinmeters',
    'mne,': 'minimumelevationinmeters',
    'mxa,': 'maximumdistanceabovesurfaceinmeters',
    'mxd,': 'maximumdepthinmeters',
    'mxe,': 'maximumelevationinmeters',
    'nat,': 'nameaccordingto',
    'natid,': 'nameaccordingtoid',
    'nc,': 'nomenclaturalcode',
    'np,': 'namepublishedin',
    'npid,': 'namepublishedinid',
    'ns,': 'nomenclaturalstatus',
    'ocn,': 'othercatalognumbers',
    'od,': 'occurrencedetails',
    'oic,': 'ownerinstitutioncode',
    'oid,': 'occurrenceid',
    'onu,': 'originalnameusage',
    'onuid,': 'originalnameusageid',
    'or,': 'occurrenceremarks',
    'ord,': 'order',
    'os,': 'occurrencestatus',
    'p,': 'preparations',
    'ph,': 'phylum',
    'pi,': 'previousidentifications',
    'pnu,': 'parentnameusage',
    'pnuid,': 'parentnameusageid',
    'prsf,': 'pointradiusspatialfit',
    'r,': 'rights',
    'rb,': 'recordedby',
    'rc,': 'reproductivecondition',
    'rh,': 'rightsholder',
    'rn,': 'recordnumber',
    'sdy,': 'startdayofyear',
    'se,': 'specificepithet',
    'sg,': 'subgenus',
    'sid,': 'scientificnameid',
    'sme,': 'samplingeffort',
    'smp,': 'samplingprotocol',
    'sn,': 'scientificname',
    'sna,': 'scientificnameauthorship',
    'sp,': 'stateprovince',
    'sx,': 'sex',
    't,': 'type',
    'tcid,': 'taxonconceptid',
    'tid,': 'taxonid',
    'tr,': 'taxonrank',
    'trm,': 'taxonremarks',
    'ts,': 'taxonomicstatus',
    'vc,': 'verbatimcoordinates',
    'vcs,': 'verbatimcoordinatesystem',
    'vd,': 'verbatimdepth',
    've,': 'verbatimelevation',
    'ved,': 'verbatimeventdate',
    'vl,': 'verbatimlocality',
    'vlat,': 'verbatimlatitude',
    'vlng,': 'verbatimlongitude',
    'vn,': 'vernacularname',
    'vs,': 'verbatimsrs',
    'vtr,': 'verbatimtaxonrank',
    'w,': 'waterbody',
    'y': 'year'
}

DWC = {'datasetname': 'dn,', 'occurrenceremarks': 'or,', 'namepublishedin': 'np,', 'geologicalcontextid': 'gid,', 'associatedreferences': 'ar,', 'month': 'm,', 'decimallongitude': 'dlng,', 'fieldnotes': 'fnt,', 'verbatimlongitude': 'vlng,', 'highergeography': 'hg,', 'modified': 'md,', 'startdayofyear': 'sdy,', 'minimumelevationinmeters': 'mne,', 'continent': 'ct,', 'recordedby': 'rb,', 'group': 'grp,', 'accessrights': 'ar,', 'locationid': 'lid,', 'maximumdistanceabovesurfaceinmeters': 'mxa,', 'kingdom': 'k,', 'verbatimeventdate': 'ved,', 'coordinateprecision': 'cp,', 'verbatimcoordinatesystem': 'vcs,', 'verbatimsrs': 'vs,', 'parentnameusageid': 'pnuid,', 'latesteraorhighesterathem': 'lera,', 'day': 'd,', 'identificationid': 'idid,', 'occurrenceid': 'oid,', 'earliestageorloweststage': 'eage,', 'earliesteonorlowesteonothem': 'eeon,', 'footprintsrs': 'fps,', 'samplingeffort': 'sme,', 'identificationqualifier': 'iq,', 'originalnameusageid': 'onuid,', 'datageneralizations': 'dg,', 'coordinateuncertaintyinmeters': 'cu,', 'higherclassification': 'hc,', 'habitat': 'h,', 'lifestage': 'ls,', 'namepublishedinid': 'npid,', 'collectioncode': 'cc,', 'latestageorhigheststage': 'lage,', 'earliestperiodorlowestsystem': 'ep,', 'verbatimlatitude': 'vlat,', 'year': 'y', 'specificepithet': 'se,', 'verbatimtaxonrank': 'vtr,', 'basisofrecord': 'br,', 'geodeticdatum': 'gd,', 'latesteonorhighesteonothem': 'leon,', 'acceptednameusage': 'anu,', 'parentnameusage': 'pnu,', 'earliesteraorlowesterathem': 'eera,', 'samplingprotocol': 'smp,', 'taxonid': 'tid,', 'formation': 'frm,', 'disposition': 'dsp,', 'language': 'ln,', 'institutionid': 'iid,', 'island': 'i,', 'occurrencestatus': 'os,', 'ownerinstitutioncode': 'oic,', 'nomenclaturalstatus': 'ns,', 'genus': 'g,', 'datasetid': 'did,', 'georeferenceprotocol': 'gp,', 'eventremarks': 'er,', 'family': 'fm,', 'scientificnameid': 'sid,', 'stateprovince': 'sp,', 'municipality': 'mn,', 'nameaccordingtoid': 'natid,', 'county': 'co,', 'phylum': 'ph,', 'associatedoccurrences': 'ao,', 'georeferencedby': 'gb,', 'earliestepochorlowestseries': 'eep,', 'taxonrank': 'tr,', 'verbatimlocality': 'vl,', 'identificationreferences': 'irf,', 'countrycode': 'cnc,', 'institutioncode': 'ic,', 'highergeographyid': 'hgid,', 'latestperiodorhighestsystem': 'lp,', 'maximumelevationinmeters': 'mxe,', 'nameaccordingto': 'nat,', 'typestatus': 'ts,', 'type': 't,', 'taxonconceptid': 'tcid,', 'eventid': 'eid,', 'eventtime': 'et,', 'islandgroup': 'ig,', 'verbatimdepth': 'vd,', 'preparations': 'p,', 'pointradiusspatialfit': 'prsf,', 'georeferenceremarks': 'gr,', 'footprintspatialfit': 'fpsf,', 'rights': 'r,', 'dynamicproperties': 'dp,', 'georeferenceverificationstatus': 'gvs,', 'sex': 'sx,', 'infraspecificepithet': 'ise,', 'bed': 'bd,', 'fieldnumber': 'fnm,', 'behavior': 'b,', 'country': 'cn,', 'taxonomicstatus': 'ts,', 'taxonremarks': 'trm,', 'eventdate': 'ed,', 'individualcount': 'ic,', 'verbatimelevation': 've,', 'rightsholder': 'rh,', 'subgenus': 'sg,', 'bibliographiccitation': 'bc,', 'verbatimcoordinates': 'vc,', 'georeferencesources': 'gs,', 'nomenclaturalcode': 'nc,', 'waterbody': 'w,', 'dateidentified': 'di,', 'catalognumber': 'cat,', 'originalnameusage': 'onu,', 'locality': 'l,', 'member': 'mem,', 'locationremarks': 'lr,', 'minimumdistanceabovesurfaceinmeters': 'mna,', 'informationwithheld': 'iw,', 'scientificnameauthorship': 'sna,', 'recordnumber': 'rn,', 'occurrencedetails': 'od,', 'lowestbiostratigraphiczone': 'lbz,', 'collectionid': 'cid,', 'acceptednameusageid': 'anuid,', 'individualid': 'indid,', 'footprintwkt': 'fp,', 'maximumdepthinmeters': 'mxd,', 'scientificname': 'sn,', 'highestbiostratigraphiczone': 'hbz,', 'class': 'cl,', 'vernacularname': 'vn,', 'previousidentifications': 'pi,', 'identificationremarks': 'irm,', 'decimallatitude': 'dlat,', 'minimumdepthinmeters': 'mnd,', 'latestepochorhighestseries': 'lep,', 'locationaccordingto': 'lcat,', 'othercatalognumbers': 'ocn,', 'establishmentmeans': 'em,', 'identifiedby': 'ib,', 'associatedmedia': 'am,', 'associatedsequences': 'as,', 'associatedtaxa': 'at,', 'lithostratigraphicterms': 'lt,', 'reproductivecondition': 'rc,', 'order': 'ord,', 'enddayofyear': 'edy,'}

def pretty_date(time=False):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    """

    import datetime as dt

    now = datetime.now()
    if type(time) is int:
        diff = now - datetime.fromtimestamp(time)
    elif not time:
        diff = now - now
    else:
        diff = now - time

    if type(diff) is dt.timedelta:
        second_diff = diff.seconds
        day_diff = diff.days
    else:
        second_diff = diff.second
        day_diff = diff.day

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "just now"
        if second_diff < 60:
            return str(second_diff) + " seconds ago"
        if second_diff < 120:
            return  "a minute ago"
        if second_diff < 3600:
            return str(second_diff / 60) + " minutes ago"
        if second_diff < 7200:
            return "an hour ago"
        if second_diff < 86400:
            return str(second_diff / 3600) + " hours ago"
    if day_diff == 1:
        return "Yesterday"
    if day_diff < 7:
        return str(day_diff) + " days ago"
    if day_diff < 31:
        return str(day_diff / 7) + " weeks ago"
    if day_diff < 365:
        return str(day_diff / 30) + " months ago"
    return str(day_diff / 365) + " years ago"

class BaseHandler(webapp.RequestHandler):
    """Base handler for handling common stuff like template rendering."""
    def render_template(self, file, template_args):
        path = os.path.join(os.path.dirname(__file__), "templates", file)
        self.response.out.write(template.render(path, template_args))
    def push_html(self, file):
        path = os.path.join(os.path.dirname(__file__), "html", file)
        self.response.out.write(open(path, 'r').read())
