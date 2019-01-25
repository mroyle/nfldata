package com.doit;

import lombok.Data;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class PlayMapperTest extends Assert {
    @Test
    public void shouldSplitLineAndReturnAPlayObject(){
        String testLine = "113,2009091000,PIT,TEN,PIT,home,TEN,PIT,56,2009-09-10,815,1715,3515,Half1,0,1,0,1,3,0,13:35,PIT 44,8,2,(13:35) (Shotgun) B.Roethlisberger pass incomplete deep right to M.Wallace. COVERAGE BY #24 HOPE,pass,0,1,0,1,0,0,0,deep,right,34,NA,NA,NA,NA,NA,NA,NA,3,3,0,NA,NA,3,3,0,0,0,0,0,0,0,0,0.0014338646477691478,0.14908828017633333,0.0019439625133838576,0.23480084556603076,0.28933588304645974,0.004776405474203797,0.3186207585758195,0,0,1.0131470856005402,-1.7125834938947782,-1.0229622619055143,1.0229622619055143,-1.4027598314188185,1.4027598314188185,-1.6346763358781586,1.6346763358781586,3.4125723251720945,-5.1251558190668725,0,0,-0.9387353554295581,0.9387353554295581,1.0166425134461776,-1.0166425134461776,2.4738369697425364,-2.4738369697425364,-4.108513305620695,4.108513305620695,0.5107930906986009,0.4892069093013991,0.5107930906986009,0.4892069093013991,-0.0495759766871644,0.4612171140114365,0.5387828859885635,-0.04029472646288734,0.04029472646288734,-0.04492099923797388,0.04492099923797388,0.10992541689995661,-0.159501393587121,0,0,-0.0283833736796667,0.0283833736796667,0.033038351128857224,-0.033038351128857224,0.0815420432202899,-0.0815420432202899,-0.12646304245826379,0.12646304245826379,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,00-0022924,B.Roethlisberger,00-0026901,M.Wallace,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,0,NA,NA,NA,NA,0,NA,NA,0,0,0,0";

        PlayOutputReceiver output = new PlayOutputReceiver();
        PlayMapper mapper = new PlayMapper();

        mapper.processElement(testLine, output);
        assertNotNull(output.getOutputPlayObject());
        System.out.println(output.getOutputPlayObject());
    }

    @Test
    public void shouldIgnoreCommasInDescriptionColumn(){
        String testLine = "113,2009091000,PIT,TEN,PIT,home,TEN,PIT,56,2009-09-10,815,1715,3515,Half1,0,1,0,1,3,0,13:35,PIT 44,8,2,\"(13:35) (Shotgun) B.Roethlisberger pass incomplete deep right to M.Wallace, COVERAGE BY #24 HOPE\",pass,0,1,0,1,0,0,0,deep,right,34,NA,NA,NA,NA,NA,NA,NA,3,3,0,NA,NA,3,3,0,0,0,0,0,0,0,0,0.0014338646477691478,0.14908828017633333,0.0019439625133838576,0.23480084556603076,0.28933588304645974,0.004776405474203797,0.3186207585758195,0,0,1.0131470856005402,-1.7125834938947782,-1.0229622619055143,1.0229622619055143,-1.4027598314188185,1.4027598314188185,-1.6346763358781586,1.6346763358781586,3.4125723251720945,-5.1251558190668725,0,0,-0.9387353554295581,0.9387353554295581,1.0166425134461776,-1.0166425134461776,2.4738369697425364,-2.4738369697425364,-4.108513305620695,4.108513305620695,0.5107930906986009,0.4892069093013991,0.5107930906986009,0.4892069093013991,-0.0495759766871644,0.4612171140114365,0.5387828859885635,-0.04029472646288734,0.04029472646288734,-0.04492099923797388,0.04492099923797388,0.10992541689995661,-0.159501393587121,0,0,-0.0283833736796667,0.0283833736796667,0.033038351128857224,-0.033038351128857224,0.0815420432202899,-0.0815420432202899,-0.12646304245826379,0.12646304245826379,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,00-0022924,B.Roethlisberger,00-0026901,M.Wallace,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,0,NA,NA,NA,NA,0,NA,NA,0,0,0,0";

        PlayOutputReceiver output = new PlayOutputReceiver();
        PlayMapper mapper = new PlayMapper();

        mapper.processElement(testLine, output);
        assertNotNull(output.getOutputPlayObject());
    }

    @Test
    public void shouldIgnoreHeaderBecauseNumberColumnsWillNotBeNumbers(){
        String testLine = "play_id,game_id,home_team,away_team,posteam,posteam_type,defteam,side_of_field,yardline_100,game_date,quarter_seconds_remaining,half_seconds_remaining,game_seconds_remaining,game_half,quarter_end,drive,sp,qtr,down,goal_to_go,time,yrdln,ydstogo,ydsnet,desc,play_type,yards_gained,shotgun,no_huddle,qb_dropback,qb_kneel,qb_spike,qb_scramble,pass_length,pass_location,air_yards,yards_after_catch,run_location,run_gap,field_goal_result,kick_distance,extra_point_result,two_point_conv_result,home_timeouts_remaining,away_timeouts_remaining,timeout,timeout_team,td_team,posteam_timeouts_remaining,defteam_timeouts_remaining,total_home_score,total_away_score,posteam_score,defteam_score,score_differential,posteam_score_post,defteam_score_post,score_differential_post,no_score_prob,opp_fg_prob,opp_safety_prob,opp_td_prob,fg_prob,safety_prob,td_prob,extra_point_prob,two_point_conversion_prob,ep,epa,total_home_epa,total_away_epa,total_home_rush_epa,total_away_rush_epa,total_home_pass_epa,total_away_pass_epa,air_epa,yac_epa,comp_air_epa,comp_yac_epa,total_home_comp_air_epa,total_away_comp_air_epa,total_home_comp_yac_epa,total_away_comp_yac_epa,total_home_raw_air_epa,total_away_raw_air_epa,total_home_raw_yac_epa,total_away_raw_yac_epa,wp,def_wp,home_wp,away_wp,wpa,home_wp_post,away_wp_post,total_home_rush_wpa,total_away_rush_wpa,total_home_pass_wpa,total_away_pass_wpa,air_wpa,yac_wpa,comp_air_wpa,comp_yac_wpa,total_home_comp_air_wpa,total_away_comp_air_wpa,total_home_comp_yac_wpa,total_away_comp_yac_wpa,total_home_raw_air_wpa,total_away_raw_air_wpa,total_home_raw_yac_wpa,total_away_raw_yac_wpa,punt_blocked,first_down_rush,first_down_pass,first_down_penalty,third_down_converted,third_down_failed,fourth_down_converted,fourth_down_failed,incomplete_pass,interception,punt_inside_twenty,punt_in_endzone,punt_out_of_bounds,punt_downed,punt_fair_catch,kickoff_inside_twenty,kickoff_in_endzone,kickoff_out_of_bounds,kickoff_downed,kickoff_fair_catch,fumble_forced,fumble_not_forced,fumble_out_of_bounds,solo_tackle,safety,penalty,tackled_for_loss,fumble_lost,own_kickoff_recovery,own_kickoff_recovery_td,qb_hit,rush_attempt,pass_attempt,sack,touchdown,pass_touchdown,rush_touchdown,return_touchdown,extra_point_attempt,two_point_attempt,field_goal_attempt,kickoff_attempt,punt_attempt,fumble,complete_pass,assist_tackle,lateral_reception,lateral_rush,lateral_return,lateral_recovery,passer_player_id,passer_player_name,receiver_player_id,receiver_player_name,rusher_player_id,rusher_player_name,lateral_receiver_player_id,lateral_receiver_player_name,lateral_rusher_player_id,lateral_rusher_player_name,lateral_sack_player_id,lateral_sack_player_name,interception_player_id,interception_player_name,lateral_interception_player_id,lateral_interception_player_name,punt_returner_player_id,punt_returner_player_name,lateral_punt_returner_player_id,lateral_punt_returner_player_name,kickoff_returner_player_name,kickoff_returner_player_id,lateral_kickoff_returner_player_id,lateral_kickoff_returner_player_name,punter_player_id,punter_player_name,kicker_player_name,kicker_player_id,own_kickoff_recovery_player_id,own_kickoff_recovery_player_name,blocked_player_id,blocked_player_name,tackle_for_loss_1_player_id,tackle_for_loss_1_player_name,tackle_for_loss_2_player_id,tackle_for_loss_2_player_name,qb_hit_1_player_id,qb_hit_1_player_name,qb_hit_2_player_id,qb_hit_2_player_name,forced_fumble_player_1_team,forced_fumble_player_1_player_id,forced_fumble_player_1_player_name,forced_fumble_player_2_team,forced_fumble_player_2_player_id,forced_fumble_player_2_player_name,solo_tackle_1_team,solo_tackle_2_team,solo_tackle_1_player_id,solo_tackle_2_player_id,solo_tackle_1_player_name,solo_tackle_2_player_name,assist_tackle_1_player_id,assist_tackle_1_player_name,assist_tackle_1_team,assist_tackle_2_player_id,assist_tackle_2_player_name,assist_tackle_2_team,assist_tackle_3_player_id,assist_tackle_3_player_name,assist_tackle_3_team,assist_tackle_4_player_id,assist_tackle_4_player_name,assist_tackle_4_team,pass_defense_1_player_id,pass_defense_1_player_name,pass_defense_2_player_id,pass_defense_2_player_name,fumbled_1_team,fumbled_1_player_id,fumbled_1_player_name,fumbled_2_player_id,fumbled_2_player_name,fumbled_2_team,fumble_recovery_1_team,fumble_recovery_1_yards,fumble_recovery_1_player_id,fumble_recovery_1_player_name,fumble_recovery_2_team,fumble_recovery_2_yards,fumble_recovery_2_player_id,fumble_recovery_2_player_name,return_team,return_yards,penalty_team,penalty_player_id,penalty_player_name,penalty_yards,replay_or_challenge,replay_or_challenge_result,penalty_type,defensive_two_point_attempt,defensive_two_point_conv,defensive_extra_point_attempt,defensive_extra_point_conv";

        PlayOutputReceiver output = new PlayOutputReceiver();
        PlayMapper mapper = new PlayMapper();

        mapper.processElement(testLine, output);
        assertNull(output.getOutputPlayObject());
    }

    @Data
    private static class PlayOutputReceiver implements DoFn.OutputReceiver<Play> {

        private Play outputPlayObject;

        @Override
        public void output(Play output) {
            outputPlayObject = output;
        }

        @Override
        public void outputWithTimestamp(Play output, Instant timestamp) {

        }
    }
}
