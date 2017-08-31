package com.wxmimperio.hadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }


    @org.junit.Test
    public void test() {
        System.out.println("/app/hadoop/export2sdo_data/export2sdocertify/inc/v_mobile_app_channel_stat_day/v_mobile_app_channel_stat_day/20170709\t123\t000");

        String str = "lzzj_guildcoin_glog,lzzj_arenacoin_glog,lzzj_skillpoint_glog,lzzj_character_glog,lzzj_arena_solo_glog,lzzj_singlescene_glog,lzzj_nqcx_glog,lzzj_cardskill_glog,lzzj_rewardtask_glog,lzzj_homelandfast_glog,lzzj_activity_glog,lzzj_item_glog,lzzj_guildactive_glog,lzzj_gacha_glog,lzzj_sceneopen_glog,lzzj_honorcoin_glog,lzzj_homelandincome_glog,lzzj_worldboss_glog,lzzj_instance_glog,lzzj_trialcoin_glog,lzzj_questionnaire_glog,lzzj_dongtai_shuxing_jiaoyan_glog,lzzj_goldingot_glog,lzzj_guildbattle_skill_glog,lzzj_olnum_glog,lzzj_astraldust_glog,lzzj_arena_team_glog,lzzj_homelandbuild_glog,lzzj_controlswitch_glog,lzzj_character_logout_glog,lzzj_guild_member_glog,lzzj_jinbi_glog,lzzj_expproduce_glog,lzzj_money_glog,lzzj_rolejade_glog,lzzj_dps_jiance_type_glog,lzzj_character_levelup_glog,lzzj_shenqi_jinjie_glog,lzzj_gmcommond_glog,lzzj_guildbattle_card_glog,lzzj_roleequip_glog,lzzj_hometask_glog,lzzj_dongtai_shuxing_jiaoyan_shuzhi_glog,lzzj_roleequip_qianghua_glog,lzzj_shenqi_shengji_glog,lzzj_roleinspire_glog,lzzj_guildparty_glog,lzzj_medalexp_glog,lzzj_homelandcancel_glog,lzzj_teamcopy_glog,lzzj_deposit_glog,lzzj_introtask_glog,lzzj_cheat_glog,lzzj_activation_code_glog,lzzj_roleequip_tupo_glog,lzzj_chenghao_produce_glog,lzzj_dailytask_glog,lzzj_energy_glog,lzzj_guildbattle_candidates_glog,lzzj_pvpresult_glog,lzzj_zuida_shanghai_jiance_glog,lzzj_guildbattle_win_glog,lzzj_roleequip_lvup_glog,lzzj_changename_glog,lzzj_guild_glog,lzzj_guild_instance_glog,lzzj_mall_glog,lzzj_shenqi_produce_glog,lzzj_trial_tower_glog,lzzj_soulstone_glog,lzzj_medalfragment_glog,lzzj_homelandstatus_glog,lzzj_card_glog,lzzj_chenghao_star_glog,lzzj_playpoint_glog,lzzj_character_login_glog,lzzj_roleequip_jinjie_glog,lzzj_newbee_guide_glog";

        System.out.println(str.split(",",-1).length);

        System.out.println("2017-07-19 10:35, 4-13204, 791000317, 1, 1, 2017-07-19 10:35:33, 3, unknown, 1-1-m91_12930105, m91_12930105, 9187, 1001000000076, S1.安梦露, 1, 战士, 0, 0, 2017-07-19 10:35:31, 0, 0, , 139.214.144.67, 1.0.0.1495, 1.0.0.1495, , 0, TEST, 0".split(",",-1).length);
    }
}
