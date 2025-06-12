from cuemsutils.cues import ActionCue, AudioCue, DmxCue, CuemsScript, CueList, VideoCue
from cuemsutils.cues.MediaCue import Media, Region
from cuemsutils.cues.CueOutput import AudioCueOutput, VideoCueOutput
def create_dummy_script():
    target_uuid = '1f301cf8-dd03-4b40-ac17-ef0e5e7988be'
    act = ActionCue({'loop': 0, 'action_target': target_uuid, 'action_type': 'play', 'ui_properties' : {'warning' : 0}})
    ac = AudioCue({
        'master_vol': 66,
        'Media': Media({
            'file_name': 'file.ext',
            'regions': [
                Region({
                    'id': 0,
                    'loop': 1,
                    'in_time': None,
                    'out_time': None
                })
            ]
        }),
        'ui_properties' : {
            'warning': None
            }
    })
    vc = VideoCue({
        'loop': 0,
        'Media': Media({
            'file_name': 'file_video.ext',
            'regions' : [
                Region({
                    'id': 0, 'loop': 1, 'in_time': None, 'out_time': None
                })
            ]
        }),
        'ui_properties' : {
            'warning': None
            }
    })
    ac.outputs = [AudioCueOutput({
                                        "output_name": "0367f391-ebf4-48b2-9f26-000000000001_system:playback_1",
                                        "output_vol": 80,
                                        "channels": [
                                            {
                                                "channel": {
                                                    "channel_num": 0,
                                                    "channel_vol": 80
                                                }
                                            }
                                        ]
                                    })]
    
    vc.outputs = [VideoCueOutput({
                                        "output_name": "0367f391-ebf4-48b2-9f26-000000000001_0",
                                        "output_geometry": {
                                            "x_scale": 1,
                                            "y_scale": 1,
                                            "corners": {
                                                "top_left": {
                                                    "x": 0,
                                                    "y": 0
                                                },
                                                "top_right": {
                                                    "x": 0,
                                                    "y": 0
                                                },
                                                "bottom_left": {
                                                    "x": 0,
                                                    "y": 0
                                                },
                                                "bottom_right": {
                                                    "x": 0,
                                                    "y": 0
                                                }
                                            }
                                        }
                                    })]

                            
    #d_c = DmxCue(time=23, scene={0:{0:10, 1:50}, 1:{20:23, 21:255}, 2:{5:10, 6:23, 7:125, 8:200}}, init_dict={'loop' : 3})
    #d_c.outputs = {'universe0': 3}
    
    custom_cue_list = CueList({'contents': [ac]})
    custom_cue_list.append(vc)
    custom_cue_list.append(act)
    
    script = CuemsScript({'CueList': custom_cue_list})
    script.name = "Test Script"
    script.description = "This is a test script"
    script.created = None
    script.modified = None
    script['id'] = None

    script['CueList']['id'] = None
    script.cuelist['contents'][0]['id'] = None
    script.cuelist['contents'][1]['id'] = None
    script.cuelist['contents'][2]['id'] = None
    script['ui_properties'] = {
        'warning': 0,
    }
    return script