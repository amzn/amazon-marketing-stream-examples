# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from ad_api.base import Marketplaces
from enum import Enum


class AdvertisingApiRegion(str, Enum):
    NA = "NA"
    EU = "EU"
    FE = "FE"

    @staticmethod
    def get_marketplace(api_region):
        if api_region == AdvertisingApiRegion.NA:
            return Marketplaces.NA
        elif api_region == AdvertisingApiRegion.EU:
            return Marketplaces.EU
        elif api_region == AdvertisingApiRegion.FE:
            return Marketplaces.JP
        else:
            raise Exception(f"Unsupported region: {api_region}")


class DataSet(str, Enum):
    sp_traffic = "sp-traffic"
    sp_conversion = "sp-conversion"
    budget_usage = "budget-usage"
    sd_traffic = "sd-traffic"
    sd_conversion = "sd-conversion"
    sponsored_ads_campaign_diagnostics_recommendations = "sponsored-ads-campaign-diagnostics-recommendations"
    campaigns = "campaigns"
    adgroups = "adgroups"
    ads = "ads"
    targets = "targets"
    sb_traffic = "sb-traffic"
    sb_conversion = "sb-conversion"
    sb_clickstream = "sb-clickstream"
    sb_rich_media = "sb-rich-media"
    sp_budget_recommendations = "sp-budget-recommendations"


class SubscriptionUpdateEntityStatus(str, Enum):
    archived = "ARCHIVED"
