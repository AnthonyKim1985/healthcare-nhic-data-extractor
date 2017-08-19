package org.bigdatacenter.healthcarenhicdataextractor;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HealthcareNhicDataExtractorApplicationTests {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    public void testDataExtractionController() {
        TrRequestInfo requestInfo = new TrRequestInfo(677, null, null, null,
                null, null, 5, null,
                null, null, null, "test", null,
                null, null, null, 1,
                "2017-08-09 00:57:08.0", null, 1, 3, 2,
                1, 1, null, null, null,
                "3.5", 1, null, null,
                "0", "key_seq", 2013, 5, null);

        ParameterInfo parameterInfo11 = new ParameterInfo(
                2003, "nhic", "nhic_t20_2003",
                1, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo12 = new ParameterInfo(
                2003, "nhic", "nhic_t30_2003",
                1, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo13 = new ParameterInfo(
                2003, "nhic", "nhic_t40_2003",
                0, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo14 = new ParameterInfo(
                2003, "nhic", "nhic_t60_2003",
                0, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");


        ParameterInfo parameterInfo21 = new ParameterInfo(
                2013, "nhic", "nhic_t20_2013",
                1, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo22 = new ParameterInfo(
                2013, "nhic", "nhic_t30_2013",
                1, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo23 = new ParameterInfo(
                2013, "nhic", "nhic_t40_2013",
                0, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");

        ParameterInfo parameterInfo24 = new ParameterInfo(
                2013, "nhic", "nhic_t60_2013",
                0, "main_sick", "I10,I109",
                "year,trt_org_tp,key_seq,person_id,ykiho_id,recu_fr_dt,form_cd,dsbjt_cd,main_sick,sub_sick,in_pat_cors_type,offc_inj_type,recn,vscn,fst_in_pat_dt,dmd_tramt,dmd_sbrdn_amt,dmd_jbrdn_amt,dmd_ct_tot_amt,dmd_mri_tot_amt,edec_add_rt,edec_tramt,edec_sbrdn_amt,edec_jbrdn_amt,edec_ct_tot_amt,edec_mri_tot_amt,dmd_drg_no,mprsc_issue_admin_id,mprsc_grant_no,tot_pres_dd_cnt,ykiho_gubun_cd,org_type,ykiho_sido,sickbed_cnt,dr_cnt,ct_yn,mri_yn,pet_yn,sex,age_group,dth_ym,dth_code1,dth_code2,sido,sgg,ipsn_type_cd,ctrb_pt_type_cd,dfab_grd_cd,dfab_ptn_cd,dfab_reg_ym,gj_ykiho_gubun_cd,height,weight,waist,bp_high,bp_lwst,blds,tot_chole,triglyceride,hdl_chole,ldl_chole,hmg,olig_prote_cd,creatinine,sgot_ast,sgpt_alt,gamma_gtp,hchk_apop_pmh_yn,hchk_hdise_pmh_yn,hchk_hprts_pmh_yn,hchk_diabml_pmh_yn,hchk_hplpdm_pmh_yn,hchk_phss_pmh_yn,hchk_etcdse_pmh_yn,fmly_apop_patien_yn,fmly_hdise_patien_yn,fmly_hprts_patien_yn,fmly_diabml_patien_yn,fmly_cancer_patien_yn,smk_stat_type_rsps_cd,past_smk_term_rsps_cd,past_dsqty_rsps_cd,cur_smk_term_rsps_cd,cur_dsqty_rsps_cd,drnk_habit_rsps_cd,tm1_drkqty_rsps_cd,mov20_wek_freq_id,mov30_wek_freq_id,wlk30_wek_freq_id,gly_cd,olig_occu_cd,olig_ph,hchk_pmh_cd1,hchk_pmh_cd2,hchk_pmh_cd3,fmly_liver_dise_patien_yn,smk_term_rsps_cd,dsqty_rsps_cd,drnk_habit_rsps_cd_2008,tm1_drkqty_rsps_cd_2008,exerci_freq_rsps_cd");



        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        parameterInfoList.add(parameterInfo11);
        parameterInfoList.add(parameterInfo12);
        parameterInfoList.add(parameterInfo13);
        parameterInfoList.add(parameterInfo14);

        parameterInfoList.add(parameterInfo21);
        parameterInfoList.add(parameterInfo22);
        parameterInfoList.add(parameterInfo23);
        parameterInfoList.add(parameterInfo24);

        ExtractionParameter extractionParameter = new ExtractionParameter(requestInfo, parameterInfoList);
        String body = restTemplate.postForObject("/extraction/api/dataExtraction", extractionParameter, String.class);
        assertThat(body).isEqualTo("OK");
    }
}