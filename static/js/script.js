$( document ).ready(function() {
                
                $("#how_scroll").fadeOut();   
                $("#about_scroll").fadeOut();
                $("#contact_scroll").fadeOut();

                $("#how").click(function(){
                    $("#index").fadeOut();
                    $("#how_scroll").fadeIn();
                    $('#how_left').addClass('animated slideInLeft');
                    $('#how_right').addClass('animated slideInRight');
                    });
                $("#about").click(function(){
                    $("#index").fadeOut();
                    $("#about_scroll").fadeIn();
                    $('#about_left').addClass('animated slideInLeft');
                    $('#about_right').addClass('animated slideInRight');
                    });
                $("#contact").click(function(){
                    $("#index").fadeOut();
                    $("#contact_scroll").fadeIn();
                    $('#contact_left').addClass('animated slideInLeft');
                    $('#contact_right').addClass('animated slideInRight');
                    });
                
                $(".back").click(function(){
                    $(".pages").fadeOut();
                    $("#index").fadeIn();
                    $('#index_left').addClass('animated slideInLeft');
                    $('#index_right').addClass('animated slideInRight');
                    });
           
		});