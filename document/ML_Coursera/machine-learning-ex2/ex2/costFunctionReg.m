function [J, grad] = costFunctionReg(theta, X, y, lambda)
%COSTFUNCTIONREG Compute cost and gradient for logistic regression with regularization
%   J = COSTFUNCTIONREG(theta, X, y, lambda) computes the cost of using
%   theta as the parameter for regularized logistic regression and the
%   gradient of the cost w.r.t. to the parameters. 

% Initialize some useful values
m = length(y); % number of training examples

% You need to return the following variables correctly 
J = 0;
grad = zeros(size(theta));

% ====================== YOUR CODE HERE ======================
% Instructions: Compute the cost of a particular choice of theta.
%               You should set J to the cost.
%               Compute the partial derivatives and set grad to the partial
%               derivatives of the cost w.r.t. each parameter in theta

J=0;
s1=0;
for i=1:m,
	z = theta'*X(i, :)';
	hx = 1/(1+exp(-z));
	v1 = -y(i) * log(hx);
	v2 = -(1-y(i)) * log( 1 - hx );
	s1 = s1 + v1 + v2;
end;
s2=0;
for i=2:size(theta,1),
	s2 = s2 + theta(i)*theta(i);
end;
J = (s1+lambda/2*s2)/m;

for j=1:size(theta,1),
  grad(j) = 0;
  s1=0;
  for i=1:m,
	z = theta'*X(i, :)';
	hx = 1/(1+exp(-z));
	v = (hx - y(i)) * X(i,j);
	s1 = s1 + v;
  end;
  if j>=2,
	grad(j) = (s1+ lambda*theta(j))/ m;
  else
	grad(j) = s1/ m;
  end;
end;

% =============================================================

end
