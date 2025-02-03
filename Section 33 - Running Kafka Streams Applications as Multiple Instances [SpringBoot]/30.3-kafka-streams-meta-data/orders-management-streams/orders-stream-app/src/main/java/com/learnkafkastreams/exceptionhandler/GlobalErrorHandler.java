package com.learnkafkastreams.exceptionhandler;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalErrorHandler {

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail handleIllegalStateException(IllegalStateException exception){

        var problemDetail = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400),
                exception.getMessage());

        problemDetail.setProperty("additionalInfo", "Please pass a valid order type : general_orders or restaurant_orders");

        return problemDetail;
    }
}
